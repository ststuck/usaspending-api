import logging
import time
from copy import deepcopy

from django.db import transaction
from django.db.models import F
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.awards.models import TransactionNormalized
from usaspending_api.awards.models.temp import TempEsTransactionHit, TempEsTransactionHitManager
from usaspending_api.awards.v2.filters.location_filter_geocode import build_temp_es_transaction_hits_by_city
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.generic_helper import get_simple_pagination_metadata
from usaspending_api.common.helpers.sql_helpers import get_db_for_write
from usaspending_api.common.validator.award import get_internal_or_generated_award_id_model
from usaspending_api.common.validator.pagination import customize_pagination_with_sort_columns
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.common.views import APIDocumentationView

logger = logging.getLogger(__name__)


class TestTempEsTransactionHitViewSet(APIView):
    @transaction.atomic(using=get_db_for_write())
    def get(self, request, format=None):
        """
        Test creation of temp table
        """
        logger.info("starting request to TestTempEsTransactionHitViewSet")
        logger.info("Creating temp table")

        #TempEsTransactionHitManager.create_temp_table()
        logger.info("temp table created")
        logger.info("indexing temp table")
        #TempEsTransactionHitManager.index_temp_table()
        logger.info("temp table indexed")

        return Response(True)


class TempEsTransactionHitViewSet(APIView):
    @transaction.atomic(using=get_db_for_write())
    def get(self, request, format=None):
        """
        Return a list of data from a temp table
        """
        logger.info("starting request to TempEsTransactionHitsViewSet")

        # Gen a bunch of unique data to jam into that table. Use epoch time so its dated
        epoch_time = int(time.time())
        row_count = 20
        dummy_hits = [TempEsTransactionHit(award_id=((epoch_time + i) % int(row_count / 5)),
                                           transaction_id=epoch_time + i)
                      for i in range(0, row_count)]

        TempEsTransactionHitManager.create_temp_table()
        # TempEsTransactionHitManager.add_es_hits_orm(dummy_hits)
        #build_temp_es_transaction_hits_by_city("recipient_location", "PLEASANTVILLE", "USA", es_batch_size=1000)
        build_temp_es_transaction_hits_by_city("recipient_location", "WASHINGTON", "USA")
        TempEsTransactionHitManager.index_temp_table()

        hits = [(hit.award_id, hit.transaction_id) for hit in TempEsTransactionHit.objects.all()]
        return Response(hits)


class TransactionViewSet(APIDocumentationView):
    """
    endpoint_doc: /awards/transactions.md
    """
    transaction_lookup = {
        # "Display Name": "database_column"
        "id": "transaction_unique_id",
        "type": "type",
        "type_description": "type_description",
        "action_date": "action_date",
        "action_type": "action_type",
        "action_type_description": "action_type_description",
        "modification_number": "modification_number",
        "description": "description",
        "federal_action_obligation": "federal_action_obligation",
        "face_value_loan_guarantee": "face_value_loan_guarantee",
        "original_loan_subsidy_cost": "original_loan_subsidy_cost",
        # necessary columns which are only present for Django Mock Queries
        "award_id": "award_id",
        "is_fpds": "is_fpds",
    }

    def __init__(self):
        models = customize_pagination_with_sort_columns(
            list(TransactionViewSet.transaction_lookup.keys()),
            'action_date'
        )
        models.extend([
            get_internal_or_generated_award_id_model(),
            {'key': 'idv', 'name': 'idv', 'type': 'boolean', 'default': True, 'optional': True}
        ])
        self._tiny_shield_models = models
        super(TransactionViewSet, self).__init__()

    def _parse_and_validate_request(self, request_dict: dict) -> dict:
        return TinyShield(deepcopy(self._tiny_shield_models)).block(request_dict)

    def _business_logic(self, request_data: dict) -> list:
        # By this point, our award_id has been validated and cleaned up by
        # TinyShield.  We will either have an internal award id that is an
        # integer or a generated award id that is a string.
        award_id = request_data['award_id']
        award_id_column = 'award_id' if type(award_id) is int else 'award__generated_unique_award_id'
        filter = {award_id_column: award_id}

        lower_limit = (request_data["page"] - 1) * request_data["limit"]
        upper_limit = request_data["page"] * request_data["limit"]

        queryset = (TransactionNormalized.objects.all()
                    .values(*list(self.transaction_lookup.values()))
                    .filter(**filter))

        if request_data["order"] == "desc":
            queryset = queryset.order_by(F(request_data["sort"]).desc(nulls_last=True))
        else:
            queryset = queryset.order_by(F(request_data["sort"]).asc(nulls_first=True))

        rows = list(queryset[lower_limit:upper_limit + 1])
        return self._format_results(rows)

    def _format_results(self, rows):
        results = []
        for row in rows:
            unique_prefix = 'ASST_TX'
            result = {k: row[v] for k, v in self.transaction_lookup.items() if k != "award_id"}
            if result['is_fpds']:
                unique_prefix = 'CONT_TX'
            result['id'] = '{}_{}'.format(unique_prefix, result['id'])
            del result['is_fpds']
            results.append(result)
        return results

    @cache_response()
    def post(self, request: Request) -> Response:
        request_data = self._parse_and_validate_request(request.data)
        results = self._business_logic(request_data)
        page_metadata = get_simple_pagination_metadata(len(results), request_data["limit"], request_data["page"])

        response = {
            "page_metadata": page_metadata,
            "results": results[:request_data["limit"]],
        }

        return Response(response)
