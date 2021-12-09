"""
Unit tests for database queries code.

"""
from dateutil import parser
from hdx.database import Database

from hdx.freshness.emailer.utils.databasequeries import DatabaseQueries
from hdx.freshness.emailer.utils.hdxhelper import HDXHelper


class TestDatabaseQueries:
    def test_get_cur_prev_runs(self, configuration, database_failure):
        now = parser.parse("2017-02-01 19:07:30.333492")
        with Database(**database_failure) as session:
            hdxhelper = HDXHelper(
                site_url="", users=list(), organizations=list()
            )
            databasequeries = DatabaseQueries(
                session=session, now=now, hdxhelper=hdxhelper
            )
            assert databasequeries.run_numbers == [
                (0, parser.parse("2017-02-01 09:07:30.333492"))
            ]
            now = parser.parse("2017-02-02 19:07:30.333492")
            databasequeries = DatabaseQueries(
                session=session, now=now, hdxhelper=hdxhelper
            )
            assert databasequeries.run_numbers == [
                (1, parser.parse("2017-02-02 09:07:30.333492")),
                (0, parser.parse("2017-02-01 09:07:30.333492")),
            ]
            now = parser.parse("2017-01-31 19:07:30.333492")
            databasequeries = DatabaseQueries(
                session=session, now=now, hdxhelper=hdxhelper
            )
            assert databasequeries.run_numbers == list()
