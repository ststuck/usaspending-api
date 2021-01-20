import logging

from django.core.management.base import BaseCommand
from django.db import connection
from usaspending_api.common.helpers.sql_helpers import execute_sql_to_ordered_dictionary

logger = logging.getLogger("script")

FILE_C_RECORDS = """
SELECT
    ta.toptier_code,
    sa.reporting_fiscal_year,
    sa.reporting_fiscal_period,
    sa.quarter_format_flag,
    COUNT(DISTINCT faba.distinct_award_key) as number_of_unique_awards,
    array_agg(DISTINCT faba.distinct_award_key) as all_awards,
    COUNT(DISTINCT CASE
        WHEN faba.award_id IS NULL
        THEN faba.distinct_award_key
        ELSE NULL
        END
    ) as number_of_unlinked_awards,
    array_remove(array_agg(DISTINCT CASE
        WHEN faba.award_id IS NULL
        THEN faba.distinct_award_key
        ELSE NULL
        END
    ), NULL) as unlinked_awards
FROM financial_accounts_by_awards AS faba
INNER JOIN submission_attributes AS sa USING (submission_id)
INNER JOIN dabs_submission_window_schedule AS dsws ON (sa.submission_window_id = dsws.id AND dsws.submission_reveal_date <= now())
INNER JOIN treasury_appropriation_account AS taa ON taa.treasury_account_identifier = faba.treasury_account_id
INNER JOIN toptier_agency ta ON taa.funding_toptier_agency_id = ta.toptier_agency_id
WHERE faba.transaction_obligated_amount IS NOT NULL
GROUP BY ta.toptier_code, sa.reporting_fiscal_year, sa.reporting_fiscal_period, sa.quarter_format_flag
"""


def dictfetchall(cursor):
    "Returns all rows from a cursor as a dict"
    desc = cursor.description
    return [
        dict(zip([col[0] for col in desc], row))
        for row in cursor.fetchall()
    ]


class Command(BaseCommand):
    """Calculate Unlinked counts between File C and File D awards"""

    def handle(self, *args, **options):
        logger.info(f"Starting {__name__}")
        self.file_c = self.obtain_file_c_records()
        self.file_d = self.obtain_file_d_records()

        # Do more stuff here

        logger.info(f"Done!")

    def obtain_file_c_records(self):
        """Query Postgres for all active File C records"""
        logger.info(f"Obtaining File C Records")
        with connection.cursor() as cursor:
            cursor.execute(FILE_C_RECORDS)
            return dictfetchall(cursor)

    def obtain_file_d_records(self):
        """Query Elasticsearch for all active File D award records"""
        logger.info(f"Obtaining File D Records")

    def combine_award_records(self):
        """Combined File C and File D into a unified dataset"""

    def create_inset_records(self):
        """Using the unified award records, create the desired insert records"""

    def inset_db_records(self):
        """Store data in database"""
