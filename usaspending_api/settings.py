"""
For more information on this file: https://docs.djangoproject.com/en/1.11/topics/settings/
For the full list of settings and their values: https://docs.djangoproject.com/en/1.11/ref/settings/
"""

from django.utils.crypto import get_random_string
import dj_database_url
import os
import sys
from config.config_reader import fetch_configuration

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

for var, settting in fetch_configuration().items():
    globals()[var.upper()] = settting

for var in globals():
    if var.endswith("PATH") and "S3" not in var:
        globals()[var] = os.path.join(BASE_DIR, globals()[var])

# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/1.11/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = get_random_string()

if DOWNLOAD_ENV != "production":
    DATA_DICTIONARY_DOWNLOAD_URL = DATA_DICTIONARY_DOWNLOAD_URLS[0]
else:
    DATA_DICTIONARY_DOWNLOAD_URL = DATA_DICTIONARY_DOWNLOAD_URLS[1]

# Application definition

INTERNAL_IPS = ()

DEBUG_TOOLBAR_CONFIG = {"SHOW_TOOLBAR_CALLBACK": lambda request: DEBUG}



# Database
# https://docs.djangoproject.com/en/1.11/ref/settings/#databases

# import an environment variable, DATABASE_URL
# see https://github.com/kennethreitz/dj-database-url for more info

DEFAULT_DB_OPTIONS = {"OPTIONS": {"options": "-c statement_timeout={0}".format(DEFAULT_DB_TIMEOUT_IN_SECONDS * 1000)}}

DATABASES = {"default": {**dj_database_url.config(conn_max_age=CONNECTION_MAX_SECONDS), **DEFAULT_DB_OPTIONS}}

# read replica env vars... if not set, default DATABASE_URL will get used
# if only one set, this will error out (single DB should use DATABASE_URL)
if os.environ.get("DB_SOURCE") or os.environ.get("DB_R1"):
    DATABASES["db_source"] = dj_database_url.parse(os.environ.get("DB_SOURCE"), conn_max_age=CONNECTION_MAX_SECONDS)
    DATABASES["db_r1"] = dj_database_url.parse(os.environ.get("DB_R1"), conn_max_age=CONNECTION_MAX_SECONDS)
    DATABASE_ROUTERS = ["usaspending_api.routers.replicas.ReadReplicaRouter"]

# import a second database connection for ETL, connecting to the data broker
# using the environemnt variable, DATA_BROKER_DATABASE_URL - only if it is set
if os.environ.get("DATA_BROKER_DATABASE_URL") and not sys.argv[1:2] == ["test"]:
    DATABASES["data_broker"] = dj_database_url.parse(
        os.environ.get("DATA_BROKER_DATABASE_URL"), conn_max_age=CONNECTION_MAX_SECONDS
)

REST_FRAMEWORK = {
    # Use Django's standard `django.contrib.auth` permissions,
    # or allow read-only access for unauthenticated users.
    "DEFAULT_PERMISSION_CLASSES": ["rest_framework.permissions.AllowAny"],
    "DEFAULT_PAGINATION_CLASS": "usaspending_api.common.pagination.UsaspendingPagination",
    "DEFAULT_RENDERER_CLASSES": (
        "rest_framework.renderers.JSONRenderer",
        "usaspending_api.common.renderers.DocumentApiRenderer",
        "usaspending_api.common.renderers.BrowsableAPIRendererWithoutForms",
    ),
}

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "specifics": {
            "()": "pythonjsonlogger.jsonlogger.JsonFormatter",
            "format": "%(asctime)s %(filename)s %(funcName)s %(levelname)s %(lineno)s %(module)s "
            + "%(message)s %(name)s %(pathname)s",
        },
        "simpletime": {"format": "%(asctime)s - %(message)s", "datefmt": "%H:%M:%S"},
        "user_readable": {
            "()": "pythonjsonlogger.jsonlogger.JsonFormatter",
            "format": "%(timestamp)s %(status)s %(method)s %(path)s %(status_code)s %(remote_addr)s %(host)s "
            + "%(response_ms)d %(message)s %(request)s %(traceback)s %(error_msg)s",
        },
    },
    "handlers": {
        "server": {
            "level": "INFO",
            "class": "logging.handlers.WatchedFileHandler",
            "filename": os.path.join(BASE_DIR, "usaspending_api/logs/server.log"),
            "formatter": "user_readable",
        },
        "console_file": {
            "level": "INFO",
            "class": "logging.handlers.WatchedFileHandler",
            "filename": os.path.join(BASE_DIR, "usaspending_api/logs/console.log"),
            "formatter": "specifics",
        },
        "console": {"level": "INFO", "class": "logging.StreamHandler", "formatter": "simpletime"},
    },
    "loggers": {
        "server": {"handlers": ["server"], "level": "INFO", "propagate": False},
        "console": {"handlers": ["console", "console_file"], "level": "INFO", "propagate": False},
    },
}


# If caches added or renamed, edit clear_caches in usaspending_api/etl/helpers.py
CACHES = {
    "default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache", "LOCATION": "default-loc-mem-cache"},
    "locations": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache", "LOCATION": "locations-loc-mem-cache"},
}

# Cache environment - 'local', 'disabled', or 'elasticache'
CACHE_ENVIRONMENT = "disabled"

# Set up the appropriate elasticache for our environment
CACHE_ENVIRONMENTS = {
    # Elasticache settings are changed during deployment, or can be set manually
    "elasticache": {
        "BACKEND": "django_redis.cache.RedisCache",
        "LOCATION": "ELASTICACHE-CONNECTION-STRING",
        "TIMEOUT": "TIMEOUT-IN-SECONDS",
        "OPTIONS": {
            "CLIENT_CLASS": "django_redis.client.DefaultClient",
            # Note: ELASTICACHE-MASTER-STRING is currently only used by Prod and will be removed in other environments.
            "MASTER_CACHE": "ELASTICACHE-MASTER-STRING",
        },
    },
    "local": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache", "LOCATION": "locations-loc-mem-cache"},
    "disabled": {"BACKEND": "django.core.cache.backends.dummy.DummyCache"},
}

# Set the usaspending-cache to whatever our environment cache dictates
CACHES["usaspending-cache"] = CACHE_ENVIRONMENTS[CACHE_ENVIRONMENT]

# DRF extensions
REST_FRAMEWORK_EXTENSIONS = {
    # Not caching errors, these are logged to exceptions.log
    "DEFAULT_CACHE_ERRORS": False,
    # Default cache is usaspending-cache, which is set above based upon environment
    "DEFAULT_USE_CACHE": "usaspending-cache",
    "DEFAULT_CACHE_KEY_FUNC": "usaspending_api.common.cache.usaspending_key_func",
}

# Django spaghetti-and-meatballs (entity relationship diagram) settings
SPAGHETTI_SAUCE = {
    "apps": ["accounts", "awards", "financial_activities", "references", "submissions", "recipient"],
    "show_fields": False,
    "exclude": {},
    "show_proxy": False,
}
