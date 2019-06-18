"""
Models for temporary tables that are created on-demand and have a life-cycle that is only the SESSION of a
database connection
"""
import itertools
import logging

from django.db import models
from psycopg2 import sql
from typing import List

from usaspending_api.common.helpers import sql_helpers

TEMP_ES_TRANSACTION_HIT_TABLE_NAME = "temp_es_transaction_hits"
TEMP_ES_TRANSACTION_HIT_AWARD_ID_INDEX_NAME = "temp_es_award_idx"
TEMP_ES_TRANSACTION_HIT_TRANSACTION_ID_INDEX_NAME = "temp_es_transaction_idx"

logger = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 5000

class TempEsTransactionHitManager(models.Manager):
    @staticmethod
    def create_temp_table():
        connection = sql_helpers.get_connection(TempEsTransactionHit, read_only=False)
        with connection.cursor() as cursor:
            _temp_table_on_commit = "PRESERVE ROWS"
            # TODO: CONSIDER ENFORCING ALL METHODS IN THIS MANAGER TO HAVE TO RUN IN AN ATOMIC BLOCK
            if connection.in_atomic_block:
                logger.debug("In a transaction block during temp table creation. Will delete rows of table after "
                             "transaction commit")
                _temp_table_on_commit = "DELETE ROWS"
            else:
                logger.debug("No transaction during temp table creation. Will leave rows in temp table. The "
                             "table will be removed when dropped or at the end of the Postgres DB SESSION")

            # Create temp table, that clears its data after each transaction
            cursor.execute("""
                CREATE TEMP TABLE {table}(
                    id SERIAL PRIMARY KEY, 
                    award_id INTEGER,
                    transaction_id INTEGER
                ) ON COMMIT {on_commit}
                """.format(
                        table=sql.Identifier(TEMP_ES_TRANSACTION_HIT_TABLE_NAME).as_string(cursor.connection),
                        on_commit=_temp_table_on_commit
                    ))

        logger.info("created temp table")

    @staticmethod
    def add_es_hits(hits: List['TempEsTransactionHit']):
        connection = sql_helpers.get_connection(TempEsTransactionHit, read_only=False)
        with connection.cursor() as cursor:
            for hit in hits:
                insert_sql = "INSERT INTO {table} (award_id, transaction_id) VALUES (%s, %s)".format(
                    table=sql.Identifier(TEMP_ES_TRANSACTION_HIT_TABLE_NAME).as_string(cursor.connection)
                )
                cursor.execute(insert_sql, (hit.award_id, hit.transaction_id))

        logger.info("loaded temp table")

    @staticmethod
    def add_es_hits_orm(hits: List['TempEsTransactionHit'], chunk_size=DEFAULT_CHUNK_SIZE):
        # Perform bulk insert in chunks.
        hits_iter = iter(hits)
        chunk_of_hits = tuple(itertools.islice(hits_iter, chunk_size))
        chunk_num = 1
        while chunk_of_hits:
            logger.debug("    -> Streaming batch #{} with a size of {} "
                         "transaction hits into {} temp table".format(chunk_num,
                                                                      chunk_size,
                                                                      TEMP_ES_TRANSACTION_HIT_TABLE_NAME))
            TempEsTransactionHit.objects.bulk_create(chunk_of_hits)
            chunk_of_hits = tuple(itertools.islice(hits_iter, chunk_size))
            chunk_num = chunk_num + 1


    @staticmethod
    def index_temp_table():
        connection = sql_helpers.get_connection(TempEsTransactionHit, read_only=False)
        with connection.cursor() as cursor:
            # Create the Indexes to make reading/joining the table fast
            cursor.execute("CREATE INDEX {index} ON {table} USING HASH (award_id)".format(
                index=sql.Identifier(TEMP_ES_TRANSACTION_HIT_AWARD_ID_INDEX_NAME).as_string(cursor.connection),
                table=sql.Identifier(TEMP_ES_TRANSACTION_HIT_TABLE_NAME).as_string(cursor.connection)
            ))
            cursor.execute("CREATE INDEX {index} ON {table} USING HASH (transaction_id)".format(
                index=sql.Identifier(TEMP_ES_TRANSACTION_HIT_TRANSACTION_ID_INDEX_NAME).as_string(cursor.connection),
                table=sql.Identifier(TEMP_ES_TRANSACTION_HIT_TABLE_NAME).as_string(cursor.connection)
            ))

        logger.info("indexed temp table")


class TempEsTransactionHit(models.Model):
    id = models.AutoField(primary_key=True)
    award_id = models.IntegerField(db_index=True)
    transaction_id = models.IntegerField(db_index=True)

    objects = TempEsTransactionHitManager()

    class Meta:
        managed = False
        db_table = TEMP_ES_TRANSACTION_HIT_TABLE_NAME
