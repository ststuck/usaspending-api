import asyncio
import logging
import psycopg2
import subprocess

from django.core.management.base import BaseCommand
from pathlib import Path

from usaspending_api.common.data_connectors.async_sql_query import async_run_creates
from usaspending_api.common.helpers.timing_helpers import ConsoleTimer as Timer
from usaspending_api.common.matview_manager import (
    DEFAULT_MATIVEW_DIR,
    DEPENDENCY_FILEPATH,
    DROP_OLD_MATVIEWS,
    MATERIALIZED_VIEWS,
    CHUNKED_MATERIALIZED_VIEWS,
    MATVIEW_GENERATOR_FILE,
    OVERLAY_VIEWS,
)
from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string

logger = logging.getLogger("console")


class Command(BaseCommand):

    help = "Create, Run, Verify Materialized View SQL"

    def faux_init(self, args):
        self.matviews = MATERIALIZED_VIEWS
        self.chunked_matviews = CHUNKED_MATERIALIZED_VIEWS
        if args["only"]:
            self.matviews = {args["only"]: MATERIALIZED_VIEWS[args["only"]]}
        self.matview_dir = args["temp_dir"]
        self.no_cleanup = args["leave_sql"]
        self.remove_matviews = not args["leave_old"]
        self.run_dependencies = args["dependencies"]
        self.chunk_count = args["chunk_count"]

    def add_arguments(self, parser):
        parser.add_argument("--only", choices=list(MATERIALIZED_VIEWS.keys()))
        parser.add_argument(
            "--leave-sql",
            action="store_true",
            help="Leave the generated SQL files instead of cleaning them after script completion.",
        )
        parser.add_argument(
            "--leave-old",
            action="store_true",
            help="Leave the old materialzied views instead of dropping them from the DB.",
        )
        parser.add_argument(
            "--temp-dir",
            type=Path,
            help="Choose a non-default directory to store materialized view SQL files.",
            default=DEFAULT_MATIVEW_DIR,
        )
        parser.add_argument(
            "--dependencies", action="store_true", help="Run the SQL dependencies before the materialized view SQL."
        )
        parser.add_argument("--chunk-count", default=10, help="Number of chunks to split the UTM into", type=int)

    def handle(self, *args, **options):
        """Overloaded Command Entrypoint"""
        with Timer(__name__):
            self.faux_init(options)
            self.generate_matview_sql()
            if self.run_dependencies:
                create_dependencies()
            self.create_views()
            if not self.no_cleanup:
                self.cleanup()

    def generate_matview_sql(self):
        """Convert JSON definition files to SQL"""
        if self.matview_dir.exists():
            logger.warning("Clearing dir {}".format(self.matview_dir))
            recursive_delete(self.matview_dir)
        self.matview_dir.mkdir()

        # IF using this for operations, DO NOT LEAVE hardcoded `python3` in the command
        # Create main list of Matview SQL files
        exec_str = f"python3 {MATVIEW_GENERATOR_FILE} --quiet --dest={self.matview_dir}/ --batch_indexes=3"
        subprocess.call(exec_str, shell=True)

        # Create SQL files for Chunked Universal Transaction Matviews
        for matview, config in self.chunked_matviews.items():
            exec_str = f"python3 {MATVIEW_GENERATOR_FILE} --quiet  --dest={self.matview_dir} --file {config['json_filepath']} --chunk-count {self.chunk_count}"
            subprocess.call(exec_str, shell=True)

    def cleanup(self):
        """Cleanup files after run"""
        recursive_delete(self.matview_dir)

    def create_views(self):
        loop = asyncio.new_event_loop()
        tasks = []

        # Create Matviews
        for matview, config in self.matviews.items():
            logger.info(f"Creating Future for matview {matview}")
            sql = (self.matview_dir / config["sql_filename"]).read_text()
            tasks.append(asyncio.ensure_future(async_run_creates(sql, wrapper=Timer(matview)), loop=loop))

        # Create Chunked Matviews
        for matview, config in self.chunked_matviews.items():
            for current_chunk in range(0, self.chunk_count):
                chunked_matview = f"{matview}_{current_chunk}"
                logger.info(f"Creating Future for chunked matview {chunked_matview}")
                sql = (self.matview_dir / f"{chunked_matview}.sql").read_text()
                tasks.append(asyncio.ensure_future(async_run_creates(sql, wrapper=Timer(chunked_matview)), loop=loop))

        loop.run_until_complete(asyncio.gather(*tasks))
        loop.close()

        for view in OVERLAY_VIEWS:
            run_sql(view.read_text(), "Creating Views")

        if self.remove_matviews:
            run_sql(DROP_OLD_MATVIEWS.read_text(), "Drop Old Materialized Views")


def create_dependencies():
    run_sql(DEPENDENCY_FILEPATH.read_text(), "dependencies")


def run_sql(sql, name):
    with psycopg2.connect(dsn=get_database_dsn_string()) as connection:
        with connection.cursor() as cursor:
            with Timer(name):
                cursor.execute(sql)


def recursive_delete(path):
    """Remove file or directory (clear entire dir structure)"""
    path = Path(str(path)).resolve()  # ensure it is an absolute path
    if not path.exists() and len(str(path)) < 6:  # don't delete the entire dir
        return
    if path.is_dir():
        for f in path.glob("*"):
            recursive_delete(f)
        path.rmdir()
    elif not path.is_dir():
        path.unlink()
    else:
        logger.info("Nothing to delete")
