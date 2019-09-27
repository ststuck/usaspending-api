import logging

from django.core.management.base import BaseCommand
from django.db import connections, transaction, DEFAULT_DB_ALIAS
from pathlib import Path

from usaspending_api.common.helpers.date_helper import cast_datetime_to_naive, datetime_command_line_argument_type
from usaspending_api.common.helpers.timing_helpers import Timer

logger = logging.getLogger("console")


CREATE_TEMP_TABLE = """
CREATE UNLOGGED TABLE IF NOT EXISTS {table} (
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

GET_MIN_MAX_FABS_SQL_STRING = """
SELECT
    MIN(published_award_financial_assistance_id), MAX(published_award_financial_assistance_id)
FROM
    published_award_financial_assistance
"""

GET_MIN_MAX_FPDS_SQL_STRING = """
SELECT
    MIN(detached_award_procurement_id), MAX(detached_award_procurement_id)
FROM
    detached_award_procurement
"""


class Command(BaseCommand):
    help = "Compare Transaction records from the source system to a USAspending DB"
    chunk_size = 250000
    temp_table = "temp_dev3319_transactions_with_diff"
    transaction_types = ["fabs", "fpds"]

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

        self.fabs = {"min_max_sql": GET_MIN_MAX_FABS_SQL_STRING, "sql": "", "diff_sql_file": "fabs_diff_select.sql"}
        self.fpds = {"min_max_sql": GET_MIN_MAX_FPDS_SQL_STRING, "sql": "", "diff_sql_file": "fpds_diff_select.sql"}

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
        db_cursor = connections[DEFAULT_DB_ALIAS].cursor()
        if self.drop_table:
            db_cursor.execute("DROP TABLE IF EXISTS {}".format(self.temp_table))
        db_cursor.execute(CREATE_TEMP_TABLE.format(table=self.temp_table))

    def read_sql(self, transaction_type):
        p = Path(self.script_dir).joinpath(getattr(self, transaction_type)["diff_sql_file"])
        with p.open() as f:
            return "".join(f.readlines())

    def create_min_max_sql(self, sql):
        predicate = []
        if self.lower_datetime_bound:
            predicate.append("updated_at >= '{}'".format(cast_datetime_to_naive(self.lower_datetime_bound)))
        if self.upper_datetime_bound:
            predicate.append("updated_at <= '{}'".format(cast_datetime_to_naive(self.upper_datetime_bound)))
        if predicate:
            sql += " WHERE " + " AND ".join(p for p in predicate)

        return sql

    def return_min_max_ids(self, sql, cursor):
        cursor.execute(sql)
        results = cursor.fetchall()
        min_id, max_id = results[0]
        self.starting_id = self.starting_id or min_id
        self.ending_id = self.ending_id or max_id
        return self.starting_id, self.ending_id

    def runner(self, transaction_type):
        func_config = getattr(self, transaction_type)

        cursor = connections["data_broker"].cursor()
        fetch_id_sql = self.create_min_max_sql(func_config["min_max_sql"])
        min_id, max_id = self.return_min_max_ids(fetch_id_sql, cursor)
        total = max_id - min_id + 1

        logger.info("Min {} ID: {:,}".format(transaction_type, min_id), transaction_type)
        logger.info("Max {} ID: {:,}".format(transaction_type, max_id), transaction_type)
        logger.info("=====> IDs in range: {:,} <=====".format(total), transaction_type)

        db_cursor = connections[DEFAULT_DB_ALIAS].cursor()
        _min = min_id
        while _min <= max_id:
            _max = min(_min + self.chunk_size - 1, max_id)
            progress = (_max - min_id + 1) / total
            sql = func_config["sql"].format(minid=_min, maxid=_max)
            query = "INSERT INTO {table} {sql}".format(table=self.temp_table, sql=sql)

            logger.info("Processing records with IDs ({:,} => {:,})".format(_min, _max), transaction_type)
            with Timer() as chunk_timer:
                db_cursor.execute(query)
            transaction.commit()

            duration = chunk_timer.as_string(chunk_timer.elapsed)
            est_completion = chunk_timer.estimated_remaining_runtime(progress)
            logger.info("---> Iteration Duration: {}".format(duration), transaction_type)
            logger.info("---> Est. Completion: {}".format(est_completion), transaction_type)

            _min = _max + 1  # Move to next chunk

        logger.info("Completed execution on {}".format(transaction_type), transaction_type)


def create_indexes(target_table):
    indexes = [
        "CREATE INDEX IF NOT EXISTS ix_{table}_action_date ON {table} USING BTREE(action_date, system) WITH (fillfactor=99)",
        "CREATE INDEX IF NOT EXISTS ix_{table}_broker_rec_create ON {table} USING BTREE(broker_record_create) WITH (fillfactor=99)",
        "CREATE INDEX IF NOT EXISTS ix_{table}_broker_rec_update ON {table} USING BTREE(broker_record_update) WITH (fillfactor=99)",
        "CREATE INDEX IF NOT EXISTS ix_{table}_diff_jsonb ON {table} USING gin(fields_diff_json jsonb_path_ops)",
        "CREATE INDEX IF NOT EXISTS ix_{table}_piid_fain_uri ON {table} USING BTREE(piid_fain_uri, system) WITH (fillfactor=99)",
        "CREATE INDEX IF NOT EXISTS ix_{table}_usa_rec_update ON {table} USING BTREE(usaspending_record_update) WITH (fillfactor=99)",
        "CREATE INDEX IF NOT EXISTS ix_{table}_usa_record_create ON {table} USING BTREE(usaspending_record_create) WITH (fillfactor=99)",
        "CREATE UNIQUE INDEX IF NOT EXISTS ix_{table}_transaction_id ON {table} USING BTREE(transaction_id) WITH (fillfactor=99)",
    ]

    db_cursor = connections[DEFAULT_DB_ALIAS].cursor()
    for index in indexes:
        sql = index.format(table=target_table)
        logger.info("running '{}'".format(sql))
        db_cursor.execute(sql)
