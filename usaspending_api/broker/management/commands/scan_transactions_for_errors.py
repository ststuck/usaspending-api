import logging

from django.core.management.base import BaseCommand
from django.db import connections, transaction, DEFAULT_DB_ALIAS
from pathlib import Path

from usaspending_api.common.helpers.date_helper import cast_datetime_to_naive, datetime_command_line_argument_type
from usaspending_api.common.helpers.timing_helpers import Timer

logger = logging.getLogger("console")


CREATE_TEMP_TABLE = """
CREATE TABLE IF NOT EXISTS {table} (
    system text,
    transaction_id bigint,
    broker_surrogate_id bigint,
    broker_derived_unique_key text,
    piid_fain_uri text,
    unique_award_key text,
    action_date date,
    record_last_modified date,
    broker_record_create timestamp with time zone,
    broker_record_update timestamp with time zone,
    usaspending_record_create timestamp with time zone,
    usaspending_record_update timestamp with time zone,
    fields_diff_json jsonb
)
"""

GET_MIN_MAX_SQL_STRING = """
{cte}
SELECT
    MIN({id_column}),
    MAX({id_column})
FROM
    {table}
"""


class Command(BaseCommand):
    help = "Compare Transaction records from the source system to a USAspending DB"
    chunk_size = 500000
    temp_table = "temp_dev3319_transactions_with_diff"
    transaction_types = ["fabs", "fpds"]
    sql_script_dir = Path(__file__).resolve().parent.parent / "sql"

    def handle(self, *args, **options):
        self.chunk_size = options["chunk_size"]
        self.drop_table = options["drop_temp_table_first"]
        self.ending_id = options["max_id"]
        self.upper_datetime_bound = options["upper_datetime_bound"]
        self.run_indexes = options["create_indexes"]
        self.lower_datetime_bound = options["lower_datetime_bound"]
        self.starting_id = options["min_id"]
        if options["one_type"]:
            self.transaction_types = [options["one_type"]]

        self.fabs = {"sql": "", "diff_sql_file": "fabs_diff_select.sql"}
        self.fpds = {"sql": "", "diff_sql_file": "fpds_diff_select.sql"}

        self.main()

    def add_arguments(self, parser):
        parser.add_argument("--chunk-size", type=int, default=self.chunk_size)
        parser.add_argument("--create-indexes", action="store_true")
        parser.add_argument("--drop-temp-table-first", action="store_true")
        parser.add_argument("--max-id", type=int)
        parser.add_argument("--min-id", type=int)
        parser.add_argument(
            "--lower-datetime-bound",
            type=datetime_command_line_argument_type(naive=True),  # Broker date/times are naive.
            help="Processes transactions updated on or after the UTC date/time "
            "provided. yyyy-mm-dd hh:mm:ss is always a safe format. Wrap in "
            "quotes if date/time contains spaces.",
        )

        parser.add_argument(
            "--upper-datetime-bound",
            type=datetime_command_line_argument_type(naive=True),  # Broker date/times are naive.
            help="Processes transactions updated prior to the UTC date/time "
            "provided. yyyy-mm-dd hh:mm:ss is always a safe format. Wrap in "
            "quotes if date/time contains spaces.",
        )
        parser.add_argument("--one-type", choices=self.transaction_types)

    def main(self):
        logger.info("STARTING SCRIPT")
        self.verify_or_create_table()
        for step in self.transaction_types:
            logger.info("Running: {}".format(step))
            getattr(self, step)["sql"] = self.read_sql(step)
            self.runner(step)

        if self.run_indexes:
            self.create_indexes()

    def verify_or_create_table(self):
        with connections[DEFAULT_DB_ALIAS].cursor() as db_cursor:
            if self.drop_table:
                db_cursor.execute("DROP TABLE IF EXISTS {}".format(self.temp_table))
            db_cursor.execute(CREATE_TEMP_TABLE.format(table=self.temp_table))

    def read_sql(self, transaction_type):
        p = Path(self.sql_script_dir).joinpath(getattr(self, transaction_type)["diff_sql_file"])
        with p.open() as f:
            return "".join(f.readlines())

    def create_min_max_sql(self, transaction_type):
        cte = ""
        cte_name = "id_cte"
        predicate = []

        if transaction_type == "fpds":
            table = "detached_award_procurement"
            id_column = "detached_award_procurement_id"
        else:
            table = "published_award_financial_assistance"
            id_column = "published_award_financial_assistance_id"

        if self.lower_datetime_bound or self.upper_datetime_bound:
            cte = "WITH {cte} AS (SELECT {id_col} FROM {table}".format(cte=cte_name, id_col=id_column, table=table)
            table = cte_name
        if self.lower_datetime_bound:
            predicate.append("updated_at >= '{}'".format(cast_datetime_to_naive(self.lower_datetime_bound)))
        if self.upper_datetime_bound:
            predicate.append("updated_at <= '{}'".format(cast_datetime_to_naive(self.upper_datetime_bound)))
        if predicate:
            cte += " WHERE " + " AND ".join(p for p in predicate) + ")"

        return GET_MIN_MAX_SQL_STRING.format(table=table, id_column=id_column, cte=cte)

    def calculate_min_max_ids(self, sql, cursor):
        cursor.execute(sql)
        results = cursor.fetchall()
        min_id, max_id = results[0]
        self.starting_id = self.starting_id or min_id
        self.ending_id = self.ending_id or max_id

    def runner(self, transaction_type):
        func_config = getattr(self, transaction_type)
        with Timer() as runner_timer:

            with connections["data_broker"].cursor() as db_cursor:
                fetch_id_sql = self.create_min_max_sql(transaction_type)
                self.calculate_min_max_ids(fetch_id_sql, db_cursor)

            if not all([self.starting_id, self.ending_id]):
                logger.warn("<{}> No transactions to compare".format(transaction_type))
                return
            total = self.ending_id - self.starting_id + 1

            logger.info("<{}> Min ID: {:,}".format(transaction_type, self.starting_id))
            logger.info("<{}> Max ID: {:,}".format(transaction_type, self.ending_id))
            logger.info("<{}> =====> IDs in range: {:,} <=====".format(transaction_type, total))

            with connections[DEFAULT_DB_ALIAS].cursor() as db_cursor:
                _min = self.starting_id
                while _min <= self.ending_id:
                    _max = min(_min + self.chunk_size - 1, self.ending_id)
                    progress = (_max - self.starting_id + 1) / total

                    predicate = "{col} BETWEEN {minid} AND {maxid}"
                    if transaction_type == "fabs":
                        col = "published_award_financial_assistance_id"
                    else:
                        col = "detached_award_procurement_id"

                    additional = []

                    if self.lower_datetime_bound:
                        additional.append(
                            "updated_at >= ''{}''".format(cast_datetime_to_naive(self.lower_datetime_bound))
                        )
                    if self.upper_datetime_bound:
                        additional.append(
                            "updated_at <= ''{}''".format(cast_datetime_to_naive(self.upper_datetime_bound))
                        )
                    if additional:
                        predicate += " AND " + " AND ".join(a for a in additional)

                    sql = func_config["sql"].format(predicate=predicate.format(col=col, minid=_min, maxid=_max))

                    query = "INSERT INTO {table} {sql}".format(table=self.temp_table, sql=sql)

                    log_text = "<{}> Processing records with IDs ({:,} => {:,}) [{:,}]"
                    logger.info(log_text.format(transaction_type, _min, _max, total))
                    with Timer() as chunk_timer:
                        db_cursor.execute(query)
                    transaction.commit()

                    duration = chunk_timer.as_string(chunk_timer.elapsed)
                    est_completion = runner_timer.estimated_remaining_runtime(progress)
                    logger.info("<{}> ---> Iteration Duration: {}".format(transaction_type, duration))
                    logger.info("<{}> ---> Est. Completion: {}".format(transaction_type, est_completion))

                    _min = _max + 1  # Move to next chunk

        logger.info(
            "<{}> Completed execution in {}".format(transaction_type, runner_timer.as_string(runner_timer.elapsed))
        )


def create_indexes(target_table):
    indexes = [
        "CREATE INDEX IF NOT EXISTS ix_{table}_action_date ON {table} USING BTREE(action_date, system)",
        "CREATE INDEX IF NOT EXISTS ix_{table}_broker_rec_create ON {table} USING BTREE(broker_record_create)",
        "CREATE INDEX IF NOT EXISTS ix_{table}_broker_rec_update ON {table} USING BTREE(broker_record_update)",
        "CREATE INDEX IF NOT EXISTS ix_{table}_diff_jsonb ON {table} USING gin(fields_diff_json jsonb_path_ops)",
        "CREATE INDEX IF NOT EXISTS ix_{table}_piid_fain_uri ON {table} USING BTREE(piid_fain_uri, system)",
        "CREATE INDEX IF NOT EXISTS ix_{table}_usa_rec_update ON {table} USING BTREE(usaspending_record_update)",
        "CREATE INDEX IF NOT EXISTS ix_{table}_usa_record_create ON {table} USING BTREE(usaspending_record_create)",
        "CREATE UNIQUE INDEX IF NOT EXISTS ix_{table}_transaction_id ON {table} USING BTREE(transaction_id)",
    ]

    db_cursor = connections[DEFAULT_DB_ALIAS].cursor()
    for index in indexes:
        sql = index.format(table=target_table)
        logger.info("running '{}'".format(sql))
        db_cursor.execute(sql)
