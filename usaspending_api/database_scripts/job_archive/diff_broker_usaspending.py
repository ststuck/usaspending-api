import argparse
import logging
import math
import os
import psycopg2
import time


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
    usaspending_record_update timestamp with time zone
)
"""

GET_MIN_MAX_FABS_SQL_STRING = """
SELECT
    MIN(published_award_financial_assistance_id), MAX(published_award_financial_assistance_id)
from
    published_award_financial_assistance
"""

GET_MIN_MAX_FPDS_SQL_STRING = """
SELECT
    MIN(detached_award_procurement_id), MAX(detached_award_procurement_id)
FROM
    detached_award_procurement
"""

GLOBALS = {
    "fabs": {"min_max_sql": GET_MIN_MAX_FABS_SQL_STRING, "sql": "", "diff_sql_file": "fabs_diff_select.sql"},
    "fpds": {"min_max_sql": GET_MIN_MAX_FPDS_SQL_STRING, "sql": "", "diff_sql_file": "fpds_diff_select.sql"},
    "script": {"chunk_size": 100000, "temp_table": "temp_dev_3319_problematic_transactions"},
}


class Timer:
    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, *args, **kwargs):
        self.end = time.perf_counter()
        self.elapsed = self.end - self.start
        self.elapsed_as_string = self.pretty_print_duration(self.elapsed)

    def estimated_remaining_runtime(self, ratio):
        end = time.perf_counter()
        elapsed = end - self.start
        est = max((elapsed / ratio) - elapsed, 0.0)
        return self.pretty_print_duration(est)

    @staticmethod
    def pretty_print_duration(elapsed):
        f, s = math.modf(elapsed)
        m, s = divmod(s, 60)
        h, m = divmod(m, 60)
        return "%d:%02d:%02d.%04d" % (h, m, s, f * 10000)


def main():
    steps = ["fabs", "fpds"]
    verify_or_create_table()
    for step in steps:
        logger.info("Running: {}".format(step))
        GLOBALS[step]["sql"] = read_sql(step)
        runner(step)


def verify_or_create_table():
    with psycopg2.connect(dsn=os.environ["DATABASE_URL"]) as connection:
        with connection.cursor() as cursor:
            if GLOBALS["script"]["drop_table"]:
                cursor.execute("DROP TABLE IF EXISTS {}".format(GLOBALS["script"]["temp_table"]))
            cursor.execute(CREATE_TEMP_TABLE.format(table=GLOBALS["script"]["temp_table"]))


def read_sql(transaction_type):
    with open(GLOBALS[transaction_type]["diff_sql_file"], "r") as f:
        return "".join(f.readlines())


def log(msg, transaction_type=None):
    if transaction_type:
        logger.info("{{{}}} {}".format(transaction_type, msg))
    else:
        logger.info(msg)


def runner(transaction_type):
    func_config = GLOBALS[transaction_type]
    with psycopg2.connect(dsn=os.environ["DATA_BROKER_DATABASE_URL"]) as connection:
        with connection.cursor() as cursor:
            cursor.execute(func_config["min_max_sql"])
            results = cursor.fetchall()
            min_id, max_id = results[0]
            total = max_id - min_id + 1

            log("Min Published_Award_Financial_Assistance_ID: {:,}".format(min_id), transaction_type)
            log("Max Published_Award_Financial_Assistance_ID: {:,}".format(max_id), transaction_type)
            log("=====> IDs in range: {:,} <=====".format(total), transaction_type)

    with psycopg2.connect(dsn=os.environ["DATABASE_URL"]) as connection:
        _min = min_id
        while _min <= max_id:
            _max = min(_min + GLOBALS["script"]["chunk_size"] - 1, max_id)
            progress = (_max - min_id + 1) / total
            query = "INSERT INTO {table} {sql}".format(
                table=GLOBALS["script"]["temp_table"], sql=func_config["sql"].format(minid=_min, maxid=_max)
            )

            with Timer() as chunk_timer:
                log("Processing records with IDs ({:,} => {:,})".format(_min, _max), transaction_type)

                with connection.cursor() as cursor:
                    cursor.execute(query)

                connection.commit()

            log("---> Iteration Duration: {}".format(chunk_timer.elapsed_as_string), transaction_type)
            log("---> Est. Completion: {}".format(chunk_timer.estimated_remaining_runtime(progress)), transaction_type)

            # Move to next chunk
            _min = _max + 1

    log("Completed exection on {}".format(transaction_type), transaction_type)


if __name__ == "__main__":
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    ls = logging.StreamHandler()
    ls.setFormatter(
        logging.Formatter("[%(asctime)s] <%(levelname)s> %(message)s", datefmt="%Y/%m/%d %H:%M:%S.%f %z (%Z)")
    )
    logger.addHandler(ls)
    parser = argparse.ArgumentParser()
    parser.add_argument("--chunk-size", type=int, default=GLOBALS["script"]["chunk_size"])
    parser.add_argument("--recreate-table", action="store_true")
    args = parser.parse_args()

    GLOBALS["script"]["chunk_size"] = args.chunk_size
    GLOBALS["script"]["drop_table"] = args.recreate_table

    logger.info("STARTING SCRIPT")
    main()
