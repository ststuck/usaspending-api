import argparse
import functools
import sys
import itertools
import os
import glob
from enum import Enum

from django.apps import apps, AppConfig
from django.core.management.color import color_style
from django.core.management.base import OutputWrapper
from django.db import connections, transaction
from django.db.models.signals import pre_migrate, post_migrate

BLUE_GREEN_REPLACEABLE_OBJECT_SCHEMA = '''
    DO $$
    DECLARE
        current_search_path text;
        timestamp_string text := to_char(NOW(), 'YYYYMMDDHHMISSMS');
      	blue_green_schema text;
        prior_blue_green_schemas text[];
        old_bg_schema text;
        resulting_search_path text;
        resulting_schemas text[];
    BEGIN 
        SELECT setting INTO current_search_path FROM pg_settings WHERE name = 'search_path';
        RAISE NOTICE 'Current Search Path %', current_search_path;
        
        -- Create the new blue or green schema
        -- And set the search_path for THIS SESSION ONLY, so objects are created in the new schema
        IF current_search_path LIKE 'blue%' THEN 
        	RAISE NOTICE 'search_path shows blue schema, creating green';
            blue_green_schema := format('green_%s', timestamp_string);
            EXECUTE format('CREATE SCHEMA %s', blue_green_schema);
            EXECUTE format('SET search_path TO %s, %s', blue_green_schema, current_search_path);
        ELSIF current_search_path LIKE 'green%' THEN 
        	RAISE NOTICE 'search_path shows green schema, creating blue';
            blue_green_schema := format('blue_%s', timestamp_string);
            EXECUTE format('CREATE SCHEMA %s', blue_green_schema);
            EXECUTE format('SET search_path TO %s, %s', blue_green_schema, current_search_path);
        ELSIF current_search_path = '' THEN 
            RAISE NOTICE 'Search path is currently empty. No blue or green schema in search_path yet, adding blue: %', blue_green_schema;
            blue_green_schema := format('blue_%s', timestamp_string);
            EXECUTE format('CREATE SCHEMA %s', blue_green_schema);
            EXECUTE format('SET search_path TO %s', blue_green_schema);
        ELSE
            blue_green_schema := format('blue_%s', timestamp_string);
            RAISE NOTICE 'No blue or green schema in search_path yet, adding blue: %', blue_green_schema;
            EXECUTE format('CREATE SCHEMA %s', blue_green_schema);
            EXECUTE format('SET search_path TO %s, %s', blue_green_schema, current_search_path);
       	END IF;
    
        -- Do creation of objects. It will put them in the new schema since it is first in the search path for this SESSION
        RAISE NOTICE '... Doing creation of new objects in % ...', blue_green_schema;
        
        -- Switch schemas (blue to green, or green to blue), now that all objects are created
        IF current_search_path LIKE 'blue%' THEN 
        	RAISE NOTICE 'Switching to green schema named %, as current search_path shows blue schema', blue_green_schema;
        ELSIF current_search_path LIKE 'green%' THEN 
        	RAISE NOTICE 'Switching to blue schema named %, as current search_path shows green schema', blue_green_schema;
        ELSIF current_search_path = '' THEN 
            RAISE NOTICE 'Switching to blue schema named %, as current search_path is empty', blue_green_schema;
        ELSE
            RAISE NOTICE 'Switching to blue schema named %, as current search_path does not have blue or green set', blue_green_schema;
       	END IF;
        -- Set it at the DB level for any new SESSIONs
        EXECUTE format('ALTER DATABASE data_store_api SET search_path TO %s, public', blue_green_schema);
        -- Set it for THIS SESSION
        EXECUTE format('SET search_path TO %s, public', blue_green_schema);
    
        -- Do some testing of the new schema to verify the blue-green switch worked
        RAISE NOTICE '... Doing testing of new objects in % ...', blue_green_schema;
        
        -- Remove old blue or green schema once testing is confirmed
        EXECUTE format('SELECT array(
            SELECT nspname 
            FROM pg_catalog.pg_namespace
            WHERE nspname <> ''%s''
            AND (nspname LIKE ''blue%%'' OR nspname LIKE ''green%%'')
        )', blue_green_schema) INTO prior_blue_green_schemas;
        
        
        FOREACH old_bg_schema IN ARRAY prior_blue_green_schemas
        LOOP 
            RAISE NOTICE 'Dropping old blue/green schema % with CASCADE', old_bg_schema;
            EXECUTE format('DROP SCHEMA IF EXISTS %s CASCADE', old_bg_schema);
        END LOOP;
        
        SELECT setting INTO resulting_search_path FROM pg_settings WHERE name = 'search_path';
        SELECT array(SELECT nspname from pg_catalog.pg_namespace) INTO resulting_schemas;
        RAISE NOTICE 'Resulting Search Path %', resulting_search_path;
        RAISE NOTICE 'Resulting Schemas %', resulting_schemas; 
    END $$;
'''

class MatviewMigrationOptions(Enum):
    skip = 1
    refresh = 2
    recreate = 3

    def __str__(self):
        """String representation uses name instead of ordinal"""
        return self.name

    @staticmethod
    def get_descriptions():
        return {
            MatviewMigrationOptions.skip:
                "Do nothing with the matviews during Django migrations",
            MatviewMigrationOptions.refresh:
                "Do not re-declare the matview SQL statements and recreate "
                "them, but do invoke a refresh on each matview AFTER all "
                "migrations have run",
            MatviewMigrationOptions.recreate:
                "Drop all matviews BEFORE migrations, and recreate them "
                "AFTER migrations based on SQL files in the matviews folder"
        }

    @staticmethod
    def help():
        return '; '.join("{}: {}".format(MatviewMigrationOptions(k), v)
                         for k, v in
                         MatviewMigrationOptions.get_descriptions().items())

    @staticmethod
    def is_valid(key):
        return any(e for e in MatviewMigrationOptions if e.name == key)

    @staticmethod
    def validate(key):
        if not MatviewMigrationOptions.is_valid(key):
            raise ValueError("[{}] is not a valid Enum key value for "
                             "MatviewMigrationOptions".format(key))

    @staticmethod
    def parse_matviews_arg(key):
        MatviewMigrationOptions.validate(key)
        return MatviewMigrationOptions[key]


def parse_custom_args_for_migrate(argv):
    custom_parser = argparse.ArgumentParser(add_help=False)
    custom_parser.add_argument('--matviews',
                               default='skip',
                               type=MatviewMigrationOptions.parse_matviews_arg,
                               choices=list(MatviewMigrationOptions),
                               help=MatviewMigrationOptions.help())
    args, argv = custom_parser.parse_known_args(argv)
    DatabaseScriptsConfig.migrate_matviews = args.matviews

    # # # We can save the argument as an environmental variable, in
    # # # which case it's to retrieve from within `project.settings`,
    # # os.environ['FOO'] = args.foo
    #
    # # or we can save the variable to settings directly if it
    # # won't otherwise be overridden.
    # from django.conf import settings
    #
    # settings.foo = args.foo
    return argv


class DatabaseScriptsConfig(AppConfig):
    name = 'usaspending_api.database_scripts'

    # What operation to perform on materialized views during migration.
    # Defaults to "skip" (do nothing to them)
    # This has implications on
    migrate_matviews = MatviewMigrationOptions.skip

    pre_migrate_hooks_registry = []
    post_migrate_hooks_registry = []

    def ready(self):
        pre_migrate.connect(run_pre_migration_hooks, sender=self)
        post_migrate.connect(run_post_migration_hooks, sender=self)

    @classmethod
    def register_pre_migration_hook(cls, hook):
        if not callable(hook):
            raise TypeError("Pre-migration hooks provided must be callable "
                            "(a function or implement __call__)")
        cls.pre_migrate_hooks_registry.append(hook)

    @classmethod
    def register_post_migration_hook(cls, hook):
        if not callable(hook):
            raise TypeError("Post-migration hooks provided must be callable "
                            "(a function or implement __call__)")
        cls.post_migrate_hooks_registry.append(hook)


_stdout = OutputWrapper(sys.stdout)
_style = color_style()
_counter = itertools.count()


def say_hi_before(**kwargs):
    _stdout.write("%s / saying hi before" % next(_counter))


def say_bye_after(**kwargs):
    _stdout.write("%s / saying bye after" % next(_counter))


@transaction.atomic
def drop_views(db_alias, schema, **kwargs):
    # TODO: Need to inspect what views are still defined in the source dir
    # TODO: Try to drop:
    #       - if it fails due to dependencies: check if a version for the view is defined
    #       - if not, throw an error that it cannot be deleted because of hard dependencies on other things
    #         - perhaps inspect if the other thing that depends on it is a replaceable object, if so drop with cascade, if not (like a matview) throw error
    #       - if so, temporarily replace the view using CREATE OR REPLACE with an innocuous statement
    #         so that the OLD definitions of the view don't cause conflicts that the new would not (like a view referencing a table or col that is to be dropped/altered)
    #         - seems dangerous though. If failure happens during, you don't know what inconsistent state your views are left in.
    #         - But I guess this would happen always, since you'd usually be left with dropped views that had not yet been recreated
    # TODO: If there, skip the drop, so that CREATE OR REPLACE can be used later
    # TODO: Or perhaps do a temp


    # Use for views:
    # -- views or materialized views referencing other views/matviews/tables
    # SELECT 
    #     src_ns.nspname as src_schema
    #     , src_rel.relname as src_name
    #     , src_rel.relkind as src_type
    #     , deps.deptype
    #     , ref_ns.nspname as ref_schema
    #     , ref_rel.relname as ref_name
    #     , ref_rel.relkind as ref_type
    # FROM pg_depend deps
    # JOIN pg_rewrite rwr ON rwr.oid = deps.objid 
    # JOIN pg_class src_rel ON src_rel.oid = rwr.ev_class
    # JOIN pg_class ref_rel ON ref_rel.oid = deps.refobjid 
    # JOIN pg_namespace src_ns ON src_ns.oid = src_rel.relnamespace
    # JOIN pg_namespace ref_ns ON ref_ns.oid = ref_rel.relnamespace
    # WHERE ref_rel.oid <> src_rel.oid
    # AND ref_ns.nspname = 'public';
        
    # Use for functions:    
    # -- views or materialized views referencing functions/stored procedures
    # SELECT 
    #     src_ns.nspname as src_schema
    #     , src_rel.relname as src_name
    #     , src_rel.relkind as src_type
    #     , deps.deptype
    #     , ref_ns.nspname as ref_schema
    #     , ref_proc.proname as ref_name
    #     , 'proc' as ref_type
    # FROM pg_depend deps
    # JOIN pg_rewrite rwr ON rwr.oid = deps.objid 
    # JOIN pg_class src_rel ON src_rel.oid = rwr.ev_class
    # JOIN pg_proc ref_proc ON ref_proc.oid = deps.refobjid 
    # JOIN pg_namespace src_ns ON src_ns.oid = src_rel.relnamespace
    # JOIN pg_namespace ref_ns ON ref_ns.oid = ref_proc.pronamespace
    # JOIN (
    #     -- Finds only "user-defined" functions/procedures (i.e. none provided by installed extensions)
    #     SELECT proc.oid
    #     FROM pg_proc proc
    #     JOIN pg_namespace ns ON ns.oid = proc.pronamespace
    #     LEFT JOIN pg_depend deps ON deps.objid = proc.oid AND deps.deptype = 'e'
    #     WHERE ns.nspname = 'public'
    #     AND deps.objid IS NULL
    # ) non_ext_procs ON non_ext_procs.oid = ref_proc.oid
    # WHERE ref_ns.nspname = 'public';

    assert schema, \
        "schema argument must be specified to " \
        "indicate on which database to drop views"
    _stdout.write("Dropping all views in schema [%s] on db with alias [%s]"
                  % (schema, db_alias))

    with connections[db_alias].cursor() as cursor:
        # Find all views that are referenced by matviews
        # These views cannot be dropped. Doing so would require dropping the
        # matview and its data
        cursor.execute("""
            -- views or materialized views referencing other (user-defined) views/matviews/tables
            SELECT DISTINCT
                src_ns.nspname as src_schema
                , src_rel.relname as src_name
                , src_rel.relkind as src_type
                , deps.deptype as dep_type
                , ref_ns.nspname as ref_schema
                , ref_rel.relname as ref_name
                , ref_rel.relkind as ref_type
            FROM pg_class src_rel -- the view or matview referencing others
            JOIN pg_namespace src_ns ON src_ns.oid = src_rel.relnamespace
            JOIN pg_rewrite rwr ON rwr.ev_class = src_rel.oid -- ev_class points to the oid of the referencing (source) relation
            JOIN pg_depend deps ON deps.objid = rwr.oid  
            JOIN pg_class ref_rel ON ref_rel.oid = deps.refobjid -- refobjid points to the oid of the referenced relation
            JOIN pg_namespace ref_ns ON ref_ns.oid = ref_rel.relnamespace
            WHERE ref_rel.oid <> src_rel.oid
            AND ref_rel.oid NOT IN (
                -- Find any views or matviews provided by an extension
                SELECT rel.oid
                FROM pg_class rel
                JOIN pg_rewrite rwr ON rwr.ev_class = rel.oid
                JOIN pg_depend deps ON deps.objid = rel.oid AND deps.deptype = 'e'
                JOIN pg_extension ext ON ext.oid = deps.refobjid
            )
            AND ref_ns.nspname = %(schema)s
            AND src_rel.relkind = %(src_type)s
            AND ref_rel.relkind = %(ref_type)s;
        """, {'schema': schema, 'src_type': "m", 'ref_type': "v"})

        hard_matview_refs = [
            "Materialized View %s.%s references View %s.%s, "
            "so the View cannot be dropped without losing matview data"
            % (row[0], row[1], row[4], row[5])
            for row in cursor.fetchall()
        ]

        migrate_matviews = apps.get_app_config(
            'database_scripts').migrate_matviews
        if migrate_matviews != MatviewMigrationOptions.recreate:
            # TODO: more clear warning output, stating this will fail and how
            # TODO: this could be avoided if using recreate AND you're sure!
            _stdout.write(_style.WARNING(hard_matview_refs))

        view_drops = list_replaceable_views(db_alias, schema)
        for i in range(len(view_drops)):
            for drop in view_drops:
                _stdout.write(drop[0] + " ... ", ending='')
                cursor.execute(drop[0])
                _stdout.write(_style.SUCCESS("OK"))

# TODO: rename from "list", since this returns a series of drop statements
def list_replaceable_views(db_alias, schema):
    # Get all views in the provided schema (excluding any provided by extensions)
    # As a precaution, restrict these not to be any in 'information_schema',
    # or 'pg_catalog' schemas, and not any views starting with pg_ (like
    # pg_stat_statements)
    with connections[db_alias].cursor() as cursor:
        drop_stmt = "SELECT 'DROP VIEW IF EXISTS ' || v.relname || ';'"
        migrate_matviews = apps.get_app_config('database_scripts').migrate_matviews
        if migrate_matviews == MatviewMigrationOptions.recreate:
            # Add cascade to the drop statement, since we are going to drop and
            # recreate all matviews, so it does not matter if they get cascade-dropped
            drop_stmt = "SELECT 'DROP VIEW IF EXISTS ' || v.relname || ' CASCADE;'"

        cursor.execute(drop_stmt + """
                FROM pg_class v
                JOIN pg_namespace ns ON ns.oid = v.relnamespace
                WHERE v.relkind = 'v' 
                AND v.oid NOT IN (
                    -- Find any views or matviews provided by an extension
                    SELECT rel.oid
                    FROM pg_class rel
                    JOIN pg_rewrite rwr ON rwr.ev_class = rel.oid
                    JOIN pg_depend deps ON deps.objid = rel.oid AND deps.deptype = 'e'
                    JOIN pg_extension ext ON ext.oid = deps.refobjid
                )
                AND ns.nspname NOT IN ('information_schema', 'pg_catalog')
                AND v.relname !~ '^pg_'
                AND ns.nspname = %s;
            """, (schema,))
        return cursor.fetchall()


@transaction.atomic
def create_views(db_alias):
    views_dir = os.path.dirname(os.path.abspath(__file__))
    path = "%s/views/*.sql" % views_dir
    files = glob.glob(path)
    for file in files:
        with open(file, 'r') as sql_script:
            _stdout.write("Running sql script [%s] ... "
                          % os.path.basename(sql_script.name),
                          ending='')
            with connections[db_alias].cursor() as cursor:
                cursor.execute(sql_script.read())
        _stdout.write(_style.SUCCESS("OK"))


@transaction.atomic
def drop_matviews(db_alias, schema, **kwargs):
    matviews = apps.get_app_config('database_scripts')\
        .migrate_matviews
    _stdout.write(_style.WARNING("migrate_matviews set to: %s" % matviews))


@transaction.atomic
def create_matviews(db_alias):
    matviews = apps.get_app_config('database_scripts') \
        .migrate_matviews
    _stdout.write(_style.WARNING("migrate_matviews set to: %s" % matviews))


def run_pre_migration_hooks(sender, **kwargs):
    _stdout.write(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    _stdout.write(_style.MIGRATE_HEADING(
        "Django Migrations Wrapper - PRE-migration Hooks"))
    _stdout.write(
        "  (this custom wrapper allows pre-migration and "
        "post-migration hooks to run before/after migrations)")
    _stdout.write(_style.MIGRATE_LABEL(
        ">>>> START PRE-migration hooks..."))

    # Run all registered pre-migration hooks
    for i, pre in enumerate(DatabaseScriptsConfig.pre_migrate_hooks_registry):
        _stdout.write(_style.MIGRATE_LABEL(
            ">>>> (%s) Running: [%s] against database with alias [%s]"
            % (i, getattr(pre, '__name__', repr(pre)),
               kwargs.get('using', '*not defined'))
        ))
        pre(db_alias=kwargs.get('using'))

    _stdout.write(_style.SUCCESS(
        ">>>> DONE PRE-migration hooks."))
    _stdout.write(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n\n")


def run_post_migration_hooks(sender, **kwargs):
    _stdout.write("\n<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    _stdout.write(_style.MIGRATE_HEADING(
        "Django Migrations Wrapper - POST-migration Hooks"))
    _stdout.write(
        "  (this custom wrapper allows pre-migration and "
        "post-migration hooks to run before/after migrations)")
    _stdout.write(_style.MIGRATE_LABEL(
        "<<<< START POST-migration hooks..."))

    # Run all registered post-migration hooks
    for i, post in enumerate(DatabaseScriptsConfig.post_migrate_hooks_registry):
        _stdout.write(_style.MIGRATE_LABEL(
            "<<<< (%s) Running: [%s] against database with alias [%s]"
            % (i, getattr(post, '__name__', repr(post)),
               kwargs.get('using', '*not defined'))
        ))
        post(db_alias=kwargs.get('using'))

    _stdout.write(_style.SUCCESS(
        "<<<< DONE POST-migration hooks."))
    _stdout.write("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")


DatabaseScriptsConfig.register_pre_migration_hook(say_hi_before)
DatabaseScriptsConfig.register_pre_migration_hook(say_hi_before)
DatabaseScriptsConfig.register_post_migration_hook(say_bye_after)
DatabaseScriptsConfig.register_post_migration_hook(say_bye_after)

# Materialized Views
# TODO: Get rid of these functions. Matviews are just part of "views"
DatabaseScriptsConfig.register_pre_migration_hook(
    functools.partial(drop_matviews, schema="public"))
DatabaseScriptsConfig.register_post_migration_hook(create_matviews)

# Views
DatabaseScriptsConfig.register_pre_migration_hook(
    functools.partial(drop_views, schema="public"))
DatabaseScriptsConfig.register_post_migration_hook(create_views)




