import copy

from sys import maxsize
from typing import List, Optional

from django.conf import settings

from usaspending_api.awards.v2.lookups.lookups import award_type_mapping
from usaspending_api.common.validator.helpers import TINY_SHIELD_SEPARATOR


_TIME_PERIOD_MIN_MESSAGE = (
    "%s falls before the earliest available search date of {min}.  For data going back to %s, use either the "
    "Custom Award Download feature on the website or one of our download or bulk_download API endpoints "
    "listed on https://api.usaspending.gov/docs/endpoints."
)

_TAS_COMPONENTS_FILTER = {
    "ata": {"type": "text", "text_type": "search", "optional": True, "allow_nulls": True},
    "aid": {"type": "text", "text_type": "search", "optional": True, "allow_nulls": False},
    "bpoa": {"type": "text", "text_type": "search", "optional": True, "allow_nulls": True},
    "epoa": {"type": "text", "text_type": "search", "optional": True, "allow_nulls": True},
    "a": {"type": "text", "text_type": "search", "optional": True, "allow_nulls": True},
    "main": {"type": "text", "text_type": "search", "optional": True, "allow_nulls": False},
    "sub": {"type": "text", "text_type": "search", "optional": True, "allow_nulls": True},
}

_STANDARD_FILTER_TREE_MODEL = {
    "type": "object",
    "min": 0,
    "object_keys": {
        "require": {
            "type": "array",
            "array_type": "any",
            "models": [{"type": "array", "array_type": "text", "text_type": "search"}],
            "min": 0,
        },
        "exclude": {
            "type": "array",
            "array_type": "any",
            "models": [{"type": "array", "array_type": "text", "text_type": "search"}],
            "min": 0,
        },
    },
}

_MODELS = {
    "agencies": {
        "name": "agencies",
        "type": "array",
        "array_type": "object",
        "object_keys": {
            "type": {"type": "enum", "enum_values": ["funding", "awarding"], "optional": False},
            "tier": {"type": "enum", "enum_values": ["toptier", "subtier"], "optional": False},
            "toptier_name": {"type": "text", "text_type": "search", "optional": True},
            "name": {"type": "text", "text_type": "search", "optional": False},
        },
    },
    "award_amounts": {
        "name": "award_amounts",
        "type": "array",
        "array_type": "object",
        "object_keys": {
            "lower_bound": {"type": "float", "optional": True},
            "upper_bound": {"type": "float", "optional": True},
        },
    },
    "award_id": {"name": "award_ids", "type": "array", "array_type": "text", "text_type": "search"},
    "award_type_codes": {
        "name": "award_type_codes",
        "type": "array",
        "array_type": "enum",
        "enum_values": list(award_type_mapping.keys()) + ["no intersection"],
    },
    "contract_pricing_type_codes": {
        "name": "contract_pricing_type_codes",
        "type": "array",
        "array_type": "text",
        "text_type": "search",
    },
    "def_codes": {"name": "def_codes", "type": "array", "array_type": "text", "text_type": "search"},
    "extent_competed_type_codes": {
        "name": "extent_competed_type_codes",
        "type": "array",
        "array_type": "text",
        "text_type": "search",
    },
    "keywords": {"name": "keywords", "type": "array", "array_type": "text", "text_type": "search", "text_min": 3},
    "legal_entities": {"name": "legal_entities", "type": "array", "array_type": "integer", "array_max": maxsize},
    "limit": {"name": "limit", "type": "integer", "default": 10, "min": 1, "max": 100},
    "naics_codes": {
        "name": "naics_codes",
        "type": "any",
        "models": [
            {
                "name": "naics_codes",
                "type": "object",
                "min": 0,
                "object_keys": {
                    "require": {"type": "array", "array_type": "integer", "text_type": "search", "min": 0},
                    "exclude": {"type": "array", "array_type": "integer", "text_type": "search", "min": 0},
                },
            },
            {"type": "array", "array_type": "integer", "text_type": "search"},
        ],
    },
    "order": {"name": "order", "type": "enum", "enum_values": ("asc", "desc"), "default": "desc"},
    "page": {"name": "page", "type": "integer", "default": 1, "min": 1},
    "place_of_performance_locations": {
        "name": "place_of_performance_locations",
        "type": "array",
        "array_type": "object",
        "object_keys": {
            "country": {"type": "text", "text_type": "search", "optional": False},
            "state": {"type": "text", "text_type": "search", "optional": True},
            "zip": {"type": "text", "text_type": "search", "optional": True},
            "district": {"type": "text", "text_type": "search", "optional": True},
            "county": {"type": "text", "text_type": "search", "optional": True},
            "city": {"type": "text", "text_type": "search", "optional": True},
        },
    },
    "place_of_performance_scope": {
        "name": "place_of_performance_scope",
        "type": "enum",
        "enum_values": ["domestic", "foreign"],
    },
    "program_numbers": {"name": "program_numbers", "type": "array", "array_type": "text", "text_type": "search"},
    "psc_codes": {
        "name": "psc_codes",
        "type": "any",
        "models": [
            {"type": "array", "array_type": "text", "text_type": "search", "min": 0},
            _STANDARD_FILTER_TREE_MODEL,
        ],
    },
    "recipient_id": {"name": "recipient_id", "type": "text", "text_type": "search"},
    "recipient_locations": {
        "name": "recipient_locations",
        "type": "array",
        "array_type": "object",
        "object_keys": {
            "country": {"type": "text", "text_type": "search", "optional": False},
            "state": {"type": "text", "text_type": "search", "optional": True},
            "zip": {"type": "text", "text_type": "search", "optional": True},
            "district": {"type": "text", "text_type": "search", "optional": True},
            "county": {"type": "text", "text_type": "search", "optional": True},
            "city": {"type": "text", "text_type": "search", "optional": True},
        },
    },
    "recipient_scope": {"name": "recipient_scope", "type": "enum", "enum_values": ("domestic", "foreign")},
    "recipient_search_text": {
        "name": "recipient_search_text",
        "type": "array",
        "array_type": "text",
        "text_type": "search",
    },
    "recipient_type_names": {
        "name": "recipient_type_names",
        "type": "array",
        "array_type": "text",
        "text_type": "search",
    },
    "set_aside_type_codes": {
        "name": "set_aside_type_codes",
        "type": "array",
        "array_type": "text",
        "text_type": "search",
    },
    "sort": {"name": "sort", "type": "text", "text_type": "search"},
    "subawards": {"name": "subawards", "key": "subawards", "type": "boolean", "default": False},
    "tas_codes": {
        "name": "tas_codes",
        "type": "any",
        "models": [
            {"type": "array", "array_type": "object", "object_keys": _TAS_COMPONENTS_FILTER},
            _STANDARD_FILTER_TREE_MODEL,
        ],
    },
    "time_period": {
        "name": "time_period",
        "type": "array",
        "array_type": "object",
        "object_keys": {
            "start_date": {
                "type": "date",
                "min": settings.API_SEARCH_MIN_DATE,
                "min_exception": _TIME_PERIOD_MIN_MESSAGE % ("start_date", settings.API_MIN_DATE),
                "max": settings.API_MAX_DATE,
            },
            "end_date": {
                "type": "date",
                "min": settings.API_SEARCH_MIN_DATE,
                "min_exception": _TIME_PERIOD_MIN_MESSAGE % ("end_date", settings.API_MIN_DATE),
                "max": settings.API_MAX_DATE,
            },
            "date_type": {
                "type": "enum",
                "enum_values": ["action_date", "last_modified_date"],
                "optional": True,
                "default": "action_date",
            },
        },
    },
    "treasury_account_components": {
        "name": "treasury_account_components",
        "type": "array",
        "array_type": "object",
        "object_keys": _TAS_COMPONENTS_FILTER,
    },
}


def build_model_list(
    model_names: List[str], required_model_name: Optional[List[str]] = None, parent_object: Optional[str] = None
) -> List[dict]:
    all_models = copy.deepcopy(_MODELS)
    output_models = []
    for name in model_names:
        if name not in all_models:
            raise ValueError(f"TinyShield model does not exist for model '{name}'")

        if required_model_name is not None and name in required_model_name:
            all_models[name]["optional"] = False

        if parent_object is None:
            all_models[name]["key"] = name
        else:
            all_models[name]["key"] = f"{parent_object}{TINY_SHIELD_SEPARATOR}{name}"

        output_models.append(all_models[name])

    return output_models
