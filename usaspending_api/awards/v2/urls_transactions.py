from django.conf.urls import url
from usaspending_api.awards.v2.views.transactions import TransactionViewSet, TempEsTransactionHitViewSet, \
    TestTempEsTransactionHitViewSet, TestRoute53CnamesPocViewSet

urlpatterns = [
    url(r'^$', TransactionViewSet.as_view()),
    url(r"^es_hits/$", TempEsTransactionHitViewSet.as_view()),
    url(r"^test_es_hits/$", TestTempEsTransactionHitViewSet.as_view()),
    url(r"^test_cname_db/$", TestRoute53CnamesPocViewSet.as_view()),
]
