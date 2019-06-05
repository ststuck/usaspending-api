from django.conf.urls import url
from usaspending_api.awards.v2.views.transactions import TransactionViewSet, TempEsTransactionHitViewSet

urlpatterns = [
    url(r'^$', TransactionViewSet.as_view()),
    url(r"^es_hits/$", TempEsTransactionHitViewSet.as_view()),
]
