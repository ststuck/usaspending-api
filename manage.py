#!/usr/bin/env python
import argparse
import os
import sys

from usaspending_api.database_scripts.apps import parse_custom_args_for_migrate

if __name__ == "__main__":
    os.environ.setdefault("DDM_CONTAINER_NAME", "app")
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "usaspending_api.settings")
    try:
        from django.core.management import execute_from_command_line
    except ImportError:
        # The above import may fail for some other reason. Ensure that the
        # issue is really that Django is missing to avoid masking other
        # exceptions on Python 2.
        try:
            import django  # noqa
        except ImportError:
            raise ImportError(
                "Couldn't import Django. Are you sure it's installed and "
                "available on your PYTHONPATH environment variable? Did you "
                "forget to activate a virtual environment?"
            )
        raise

    argv = sys.argv
    cmd = argv[1] if len(argv) > 1 else None

    # Handle user-defined extra arguments providing conditional behavior for
    # customized or overrides/extensions to Django management commands
    if cmd in ['migrate']:
        argv = parse_custom_args_for_migrate(argv)

    # parse_known_args strips the extra arguments from argv,
    # so we can safely pass it to Django.
    execute_from_command_line(argv)

    #execute_from_command_line(sys.argv)
