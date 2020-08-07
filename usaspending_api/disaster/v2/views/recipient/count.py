import asyncio

from django.db.models import Count, Case, When, F, TextField, Q
from django.db.models.functions import Cast
from django_cte import With
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.data_connectors.async_sql_query import async_run_select
from usaspending_api.common.helpers.orm_helpers import CATEGORY_TO_MODEL, generate_raw_quoted_query
from usaspending_api.disaster.v2.views.disaster_base import DisasterBase
from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.disaster.v2.views.disaster_base import FabaOutlayMixin, AwardTypeMixin
from usaspending_api.recipient.v2.lookups import SPECIAL_CASES


class RecipientCountViewSet(DisasterBase, FabaOutlayMixin, AwardTypeMixin):
    """
    Obtain the count of Recipients related to supplied DEFC filter.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/recipient/count.md"

    required_filters = ["def_codes", "award_type_codes"]

    @cache_response()
    def post(self, request: Request) -> Response:

        award_filters = [~Q(total_loan_value=0) | ~Q(total_obligation_by_award=0) | ~Q(total_outlay_by_award=0)]

        if self.award_type_codes:
            award_filters.append(Q(type__in=self.award_type_codes))

        faba_filters = [
            self.all_closed_defc_submissions,
            self.is_in_provided_def_codes,
        ]

        dollar_annotations = {
            "inner_obligation": self.obligated_field_annotation,
            "inner_outlay": self.outlay_field_annotation,
        }

        cte = With(
            FinancialAccountsByAwards.objects.filter(*faba_filters).values("award_id").annotate(**dollar_annotations)
        )

        loop = asyncio.new_event_loop()
        results = {}
        for award_type, award_type_lookup in CATEGORY_TO_MODEL.items():
            model = award_type_lookup["model"]
            queryset = (
                cte.join(model, award_id=cte.col.award_id)
                .with_cte(cte)
                .annotate(
                    total_obligation_by_award=cte.col.inner_obligation, total_outlay_by_award=cte.col.inner_outlay
                )
                .filter(*award_filters)
                .annotate(
                    count=Count(
                        Case(
                            When(recipient_name__in=SPECIAL_CASES, then=F("recipient_name")),
                            default=Cast(F("recipient_hash"), output_field=TextField()),
                        ),
                        distinct=True,
                    )
                )
                .values("count")
            )
            sql = generate_raw_quoted_query(queryset)

            # Remove the "GROUP BY"; could avoid group by with "aggregate" instead of "annotate", but you cannot
            # generate the SQL from a queryset that ends with "aggregate"
            remove_groupby_string_index = sql.rfind("GROUP BY")
            results[award_type] = asyncio.ensure_future(async_run_select(sql[:remove_groupby_string_index]), loop=loop)

        all_statements = asyncio.gather(*[value for value in results.values()])
        loop.run_until_complete(all_statements)
        loop.close()

        return Response({"count": sum([v.result()[0]["count"] for v in results.values()])})
