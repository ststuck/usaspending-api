# Generated by Django 2.2.13 on 2020-06-23 17:02

from django.db import migrations


class Migration(migrations.Migration):

    # Empties table
    TRUNCATE_SQL = "truncate table dabs_submission_window_schedule restart identity"

    # WARNING - The dates for 2020 month periods 7,8,9 have been modified for the sake of testing.
    # This should be modified before being run in prod. The nightly job will overwrite the data
    # either way though.
    # Inserts base records into the table.
    INSERT_SQL = """
INSERT
	INTO
	dabs_submission_window_schedule (id, period_start_date, period_end_date, submission_start_date, submission_due_date, certification_due_date, submission_reveal_date, submission_fiscal_year, submission_fiscal_quarter, submission_fiscal_month, is_quarter )
VALUES ('2017031', '2016-10-01 00:00:00', '2016-12-31 00:00:00', '2017-01-19 00:00:00', '2017-02-19 00:00:00', '2017-02-19 00:00:00', '2017-02-20 00:00:00', 2017, 1, 3, TRUE),
('2017061', '2017-01-01 00:00:00', '2017-03-31 00:00:00', '2017-04-19 00:00:00', '2017-05-19 00:00:00', '2017-05-19 00:00:00', '2017-05-20 00:00:00', 2017, 2, 6, TRUE),
('2017091', '2017-04-01 00:00:00', '2017-06-30 00:00:00', '2017-07-19 00:00:00', '2017-08-14 00:00:00', '2017-08-14 00:00:00', '2017-08-15 00:00:00', 2017, 3, 9, TRUE),
('2017121', '2017-07-01 00:00:00', '2017-09-30 00:00:00', '2017-10-06 00:00:00', '2017-11-30 00:00:00', '2017-11-30 00:00:00', '2017-12-01 00:00:00', 2017, 4, 12, TRUE),
('2018031', '2017-10-01 00:00:00', '2017-12-31 00:00:00', '2018-01-19 00:00:00', '2018-02-14 00:00:00', '2018-02-14 00:00:00', '2018-02-15 00:00:00', 2018, 1, 3, TRUE),
('2018061', '2018-01-01 00:00:00', '2018-03-31 00:00:00', '2018-04-19 00:00:00', '2018-05-15 00:00:00', '2018-05-15 00:00:00', '2018-05-16 00:00:00', 2018, 2, 6, TRUE),
('2018091', '2018-04-01 00:00:00', '2018-06-30 00:00:00', '2018-07-19 00:00:00', '2018-08-14 00:00:00', '2018-08-14 00:00:00', '2018-08-15 00:00:00', 2018, 3, 9, TRUE),
('2018121', '2018-07-01 00:00:00', '2018-09-30 00:00:00', '2018-10-19 00:00:00', '2018-11-14 00:00:00', '2018-11-14 00:00:00', '2018-11-15 00:00:00', 2018, 4, 12, TRUE),
('2019031', '2018-10-01 00:00:00', '2018-12-31 00:00:00', '2019-02-21 00:00:00', '2019-03-20 00:00:00', '2019-03-20 00:00:00', '2019-03-21 00:00:00', 2019, 1, 3, TRUE),
('2019061', '2019-01-01 00:00:00', '2019-03-31 00:00:00', '2019-04-19 00:00:00', '2019-05-15 00:00:00', '2019-05-15 00:00:00', '2019-05-16 00:00:00', 2019, 2, 6, TRUE),
('2019091', '2019-04-01 00:00:00', '2019-06-30 00:00:00', '2019-07-19 00:00:00', '2019-08-14 00:00:00', '2019-08-14 00:00:00', '2019-08-15 00:00:00', 2019, 3, 9, TRUE),
('2019121', '2019-07-01 00:00:00', '2019-09-30 00:00:00', '2019-10-18 00:00:00', '2019-11-14 00:00:00', '2019-11-14 00:00:00', '2019-11-15 00:00:00', 2019, 4, 12, TRUE),
('2020031', '2019-10-01 00:00:00', '2019-12-31 00:00:00', '2020-01-17 00:00:00', '2020-02-14 00:00:00', '2020-02-14 00:00:00', '2020-02-15 00:00:00', 2020, 1, 3, TRUE),
('2020061', '2020-01-01 00:00:00', '2020-03-31 00:00:00', '2020-04-17 00:00:00', '2020-05-15 00:00:00', '2020-05-15 00:00:00', '2020-05-16 00:00:00', 2020, 2, 6, TRUE),
('2020091', '2020-04-01 00:00:00', '2020-06-30 00:00:00', '2020-07-17 00:00:00', '2020-08-14 00:00:00', '2020-07-30 00:00:00', '2020-07-31 00:00:00', 2020, 3, 9, TRUE),
('2020121', '2020-07-01 00:00:00', '2020-09-30 00:00:00', '2020-10-19 00:00:00', '2020-11-16 00:00:00', '2020-11-16 00:00:00', '2020-11-17 00:00:00', 2020, 4, 12, true),
('2020070', '2020-04-01 00:00:00', '2020-04-30 00:00:00', '2020-05-15 00:00:00', '2020-08-14 00:00:00', '2020-05-28 00:00:00', '2020-05-29 00:00:00', 2020, 3, 7, FALSE),
('2020080', '2020-05-01 00:00:00', '2020-05-31 00:00:00', '2020-06-16 00:00:00', '2020-08-14 00:00:00', '2020-06-29 00:00:00', '2020-06-30 00:00:00', 2020, 3, 8, FALSE),
('2020090', '2020-06-01 00:00:00', '2020-06-30 00:00:00', '2020-07-17 00:00:00', '2020-08-14 00:00:00', '2020-07-30 00:00:00', '2020-07-31 00:00:00', 2020, 3, 9, FALSE),
('2020100', '2020-07-01 00:00:00', '2020-07-31 00:00:00', '2020-08-19 00:00:00', '2020-11-16 00:00:00', '2020-08-28 00:00:00', '2020-08-29 00:00:00', 2020, 4, 10, FALSE),
('2020110', '2020-08-01 00:00:00', '2020-08-31 00:00:00', '2020-09-18 00:00:00', '2020-11-16 00:00:00', '2020-09-29 00:00:00', '2020-09-30 00:00:00', 2020, 4, 11, FALSE),
('2020120', '2020-09-01 00:00:00', '2020-09-30 00:00:00', '2020-10-19 00:00:00', '2020-11-16 00:00:00', '2020-11-16 00:00:00', '2020-11-17 00:00:00', 2020, 4, 12, FALSE)
"""

    dependencies = [
        ("submissions", "0009_dabssubmissionwindowschedule"),
    ]

    operations = [
        migrations.RunSQL(
            sql=[TRUNCATE_SQL, INSERT_SQL], reverse_sql=[TRUNCATE_SQL]
        )
    ]
