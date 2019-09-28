import logging
import random

from datetime import datetime, timezone, timedelta
from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.db import connections, DEFAULT_DB_ALIAS, transaction
from typing import List
from tempfile import NamedTemporaryFile

from usaspending_api.common.helpers.date_helper import datetime_command_line_argument_type
from usaspending_api.common.helpers.fiscal_year_helpers import (
    convert_fiscal_quarter_to_dates,
    previous_fiscal_quarter,
    fiscal_year_and_quarter_from_datetime,
)
from usaspending_api.common.helpers.timing_helpers import Timer

logger = logging.getLogger("console")

BATCH_SIZE = 75000


class Command(BaseCommand):

    average_chunk_seconds = None

    def add_arguments(self, parser):
        parser.add_argument("--closing-time", type=datetime_command_line_argument_type(naive=False), required=True)
        parser.add_argument("--diff-table", type=str, required=True)
        parser.add_argument("--system", choices=("fpds", "fabs"), required=True)
        parser.add_argument(
            "--start-datetime",
            type=datetime_command_line_argument_type(naive=False),
            help="the start week (go back in time)",
            required=True,
        )

    def handle(self, *args, **options):
        self.closing_time = options["closing_time"]
        self.system = options["system"]
        self.failed_batches = []
        self.table = options["diff_table"]

        curr_fy, curr_fq = fiscal_year_and_quarter_from_datetime(options["start_datetime"])

        logger.info("======= Starting Tony's Weekend Saver =======")
        logger.info("  [{}] Starting FY{}Q{} using {}".format(self.system, curr_fy, curr_fq, self.table))
        logger.info("  Desired End Datetime = {}".format(self.closing_time))

        iteration = 1
        all_done = False

        while not all_done:
            logger.info("===== Running FY{}Q{} =====".format(curr_fy, curr_fq))
            quarter_begin, quarter_end = convert_fiscal_quarter_to_dates(curr_fy, curr_fq)
            week_start = quarter_end
            week_end = quarter_end - timedelta(days=7)

            while not all_done:
                if len(self.failed_batches) > 2:
                    raise RuntimeError("TOO MANY FAILURES")
                if self.average_chunk_seconds:
                    curr_time = datetime.now(timezone.utc)
                    next_run_estimated_end_datetime = curr_time + timedelta(seconds=self.average_chunk_seconds)
                    if next_run_estimated_end_datetime >= self.closing_time:
                        logger.info("You don't have to go home, but you can't... stay...... heeeeere")
                        all_done = True
                        break
                    else:
                        dt_str = next_run_estimated_end_datetime.isoformat()
                        logger.info(
                            "{} before closing time".format(self.closing_time - next_run_estimated_end_datetime)
                        )
                        logger.info("Continuing with an estimated loop end datetime of: {}".format(dt_str))

                with Timer() as batch_timer:

                    self.time_runner(lower_datetime=week_end, upper_datetime=week_start)

                if self.average_chunk_seconds is None:
                    self.average_chunk_seconds = batch_timer.elapsed.seconds  # get seconds from timedelta object
                else:
                    self.average_chunk_seconds = rolling_average(
                        self.average_chunk_seconds, batch_timer.elapsed.seconds, iteration
                    )

                if week_end <= quarter_begin:
                    logger.info("=== Completed FY{}Q{} ===\n".format(curr_fy, curr_fq))
                    curr_fy, curr_fq = previous_fiscal_quarter(curr_fy, curr_fq)
                    break

                iteration += 1
                week_start = week_end
                if week_end - timedelta(days=7) < quarter_begin:
                    week_end = quarter_begin
                else:
                    week_end -= timedelta(days=7)

        if self.failed_batches:
            logger.error("Script completed with the following failures: {}".format(", ".join(self.failed_batches)))
            raise SystemExit(3)
        else:
            logger.info("Script completed with no failures")

    def time_runner(self, lower_datetime, upper_datetime):
        for batch_num, id_batch in enumerate(
            search_discrepencies_for_transactions(self.table, self.system, lower_datetime, upper_datetime)
        ):
            logger.info("Processing batch {} of {} transactions".format(batch_num, len(id_batch)))
            tempfile = NamedTemporaryFile(mode="w")
            tempfile.write("\n".join([str(id) for id in id_batch]))
            tempfile.seek(0)
            try:
                if self.system == "fabs":
                    call_command("fabs_nightly_loader", "--id-file={}".format(tempfile.name))
                elif self.system == "fpds":
                    call_command("fpds_nightly_loader", "--id-file={}".format(tempfile.name))
            except SystemExit:
                msg = "<{}> filename: {}".format(self.system, tempfile.name)
                logger.info("Loader script failed!!!! {}".format(msg))
                self.failed_batches.append(msg)
            except Exception:
                msg = "<{}> filename: {}".format(self.system, tempfile.name)
                logger.exception("Unkown exception {}".format(msg))
                self.failed_batches.append(msg)


def search_discrepencies_for_transactions(
    table: str, system: str, lower_datetime: datetime, upper_datetime: datetime
) -> List[int]:

    logger.info("Filtering <{}> between {} - {}".format(system, lower_datetime, upper_datetime))
    cursor_name = "tws_cursor_{}".format(str(random.getrandbits(128))[:5])

    predicate = "system = '{}' AND action_date BETWEEN '{}' AND '{}' ".format(system, lower_datetime, upper_datetime)
    with transaction.atomic(), connections[DEFAULT_DB_ALIAS].cursor() as db_cursor:
        sql = "DECLARE {cursor} CURSOR FOR SELECT {key} FROM {table} WHERE {predicate} "
        db_cursor.execute(sql.format(cursor=cursor_name, key="broker_surrogate_id", table=table, predicate=predicate))

        while True:
            db_cursor.execute("FETCH {next} FROM {cursor}".format(next=BATCH_SIZE, cursor=cursor_name))
            chunk = db_cursor.fetchall()
            if not chunk:
                break
            yield [row[0] for row in chunk]


def rolling_average(current_avg: float, new_value: float, total_count: int) -> float:
    """Recalculate a running (or moving) average

    Only needs the current average, a new value to include, and the total samples
    """
    new_average = float(current_avg)
    new_average -= new_average / total_count
    new_average += new_value / total_count
    return new_average
