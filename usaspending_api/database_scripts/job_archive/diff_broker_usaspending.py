import logging
import math
import os
import psycopg2
import time
from pathlib import Path


GET_MIN_MAX_FABS_SQL_STRING = """
SELECT
    min(published_award_financial_assistance_id), max(published_award_financial_assistance_id)
from
    published_award_financial_assistance
"""

GET_MIN_MAX_FPDS_SQL_STRING = """
select
    min(detached_award_procurement_id), max(detached_award_procurement_id)
from
    detached_award_procurement
"""

GLOBALS = {
    "fabs": {
        "min_max_sql": GET_MIN_MAX_FABS_SQL_STRING,
        "sql": "",
        "diff_sql_file": "fabs_diff_select.sql",
        "destination_dir": "fabs/",
        "dest_path": None,
        "file_name_pattern": "fabs_results_{}.csv",
    },
    "fpds": {
        "min_max_sql": GET_MIN_MAX_FPDS_SQL_STRING,
        "sql": "",
        "diff_sql_file": "fpds_diff_select.sql",
        "destination_dir": "fpds/",
        "dest_path": None,
        "file_name_pattern": "fpds_results_{}.csv",
    },
    "script": {"chunk_size": 500000},
}

logger = logging.getLogger()
logger.setLevel(logging.INFO)
ls = logging.StreamHandler()
ls.setFormatter(logging.Formatter("[%(asctime)s] <%(levelname)s> %(message)s", datefmt="%Y/%m/%d %H:%M:%S.%f %z (%Z)"))
logger.addHandler(ls)


class Timer:
    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, *args, **kwargs):
        self.end = time.perf_counter()
        self.elapsed = self.end - self.start
        self.elapsed_as_string = pretty_print_duration(self.elapsed)

    def estimated_remaining_runtime(self, ratio):
        end = time.perf_counter()
        elapsed = end - self.start
        est = max((elapsed / ratio) - elapsed, 0.0)
        return pretty_print_duration(est)


def pretty_print_duration(elapsed):
    f, s = math.modf(elapsed)
    m, s = divmod(s, 60)
    h, m = divmod(m, 60)
    return "%d:%02d:%02d.%04d" % (h, m, s, f * 10000)


def read_sql(data_system):
    with open(GLOBALS[data_system]["diff_sql_file"], "r") as f:
        return "".join(f.readlines())


def check_dir(data_system, wipe=False):
    path = Path(GLOBALS[data_system]["destination_dir"])
    if path.exists():
        if wipe:
            raise NotImplementedError
        elif not path.is_dir():
            raise Exception("Target Dir is not a directory!")
    else:
        path.mkdir(parents=True, exist_ok=True)
    GLOBALS[data_system]["dest_path"] = path


def main():
    steps = ["fabs", "fpds"]
    for step in steps:
        logger.info("Running: {}".format(step))
        GLOBALS[step]["sql"] = read_sql(step)
        check_dir(step)
        runner(step)


def log(msg, data_system=None):
    if data_system:
        logger.info("{{{}}} {}".format(data_system, msg))
    else:
        logger.info(msg)


def runner(data_system):
    file_number = 0
    func_config = GLOBALS[data_system]
    with psycopg2.connect(dsn=os.environ["DATA_BROKER_DATABASE_URL"]) as connection:
        with connection.cursor() as cursor:
            cursor.execute(func_config["min_max_sql"])
            results = cursor.fetchall()
            min_id, max_id = results[0]
            total = max_id - min_id + 1

            log("Min Published_Award_Financial_Assistance_ID: {:,}".format(min_id), data_system)
            log("Max Published_Award_Financial_Assistance_ID: {:,}".format(max_id), data_system)
            log("=====> IDs in range: {:,} <=====".format(total), data_system)

    with psycopg2.connect(dsn=os.environ["DATABASE_URL"]) as connection:
        _min = min_id
        while _min <= max_id:
            _max = min(_min + GLOBALS["script"]["chunk_size"] - 1, max_id)
            progress = (_max - min_id + 1) / total
            query = "copy ({}) to stdout with csv header".format(func_config["sql"].format(minid=_min, maxid=_max))

            with Timer() as chunk_timer:
                dest_file_name = func_config["file_name_pattern"].format(file_number)
                dest_file_path = func_config["dest_path"].joinpath(dest_file_name)
                log("Processing records with IDs ({:,} => {:,})".format(_min, _max), data_system)
                log("---> Storing IDs for diff transactions in {}".format(dest_file_name), data_system)
                with connection.cursor() as cursor:
                    with dest_file_path.open("w") as f:
                        cursor.copy_expert(query, f)

            log("---> Iteration Duration: {}".format(chunk_timer.elapsed_as_string), data_system)
            log("---> Est. Completion: {}".format(chunk_timer.estimated_remaining_runtime(progress)), data_system)

            # Move to next chunk
            _min = _max + 1
            file_number += 1

    log("Completed exection on {}".format(data_system), data_system)


if __name__ == "__main__":
    logger.info("STARTING SCRIPT")
    main()
