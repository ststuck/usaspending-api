import time

from django.core.management.commands import migrate


class Command(migrate.Command):
    """
    Wrapper around the core Django `migrate` Command, to allow app-specific
    pre-processing (before migrations run) and post-processing (after migrations
    run).
    """

    def handle(self, *args, **options):
        self.stdout.write(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        self.stdout.write(self.style.MIGRATE_HEADING(
            "Django Migrations Wrapper - PRE-migration Hooks"))
        self.stdout.write(
            "  (this custom wrapper allows pre-migration and "
            "post-migration hooks to run before/after migrations)")
        self.stdout.write(self.style.MIGRATE_LABEL(
            ">>>> START PRE-migration hooks..."))
        time.sleep(5)
        self.stdout.write(self.style.SUCCESS(
            ">>>> DONE PRE-migration hooks."))
        self.stdout.write(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n\n")

        # Invoke the actual Django core `migrate` management command
        super(Command, self).handle(*args, **options)

        self.stdout.write("\n<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
        self.stdout.write(self.style.MIGRATE_HEADING(
            "Django Migrations Wrapper - POST-migration Hooks"))
        self.stdout.write(
            "  (this custom wrapper allows pre-migration and "
            "post-migration hooks to run before/after migrations)")
        self.stdout.write(self.style.MIGRATE_LABEL(
            "<<<< START POST-migration hooks..."))
        time.sleep(5)
        self.stdout.write(self.style.SUCCESS(
            "<<<< DONE POST-migration hooks."))
        self.stdout.write("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")

# Alternatively, create an AppConfig class like this, with a ready() function
# It MIGHT need to have a models.py module in that app in order to pick these up
# Could name the AppConfig class "ReplaceableDbObjectsAppConfig"
# ... or put it in e.g. a database_scripts or database app folder with a standard AppConfig class?
# TODO: Need to make sure it runs for this app, and only runs once for each of pre/post signals

# SEE:
#  - AppConfig: https://docs.djangoproject.com/en/1.11/ref/applications/#django.apps.AppConfig
#  - Migrations signals: https://docs.djangoproject.com/en/1.11/ref/signals/#pre-migrate

# # anthology/apps.py
#
# from rock_n_roll.apps import RockNRollConfig
#
# class JazzManoucheConfig(RockNRollConfig):
#     verbose_name = "Jazz Manouche"
#
#     def ready(self):
#         pre_migrate.connect(my_callback, sender=self)
#         post_migrate.connect(my_callback, sender=self)
#
# # anthology/settings.py
#
# INSTALLED_APPS = [
#     'anthology.apps.JazzManoucheConfig',
#     # ...
# ]