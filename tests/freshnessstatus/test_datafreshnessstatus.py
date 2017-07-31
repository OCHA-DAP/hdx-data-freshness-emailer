# -*- coding: utf-8 -*-
'''
Unit tests for the data freshness status code.

'''
import os
import shutil
from os.path import join

import pytest

from freshnessstatus.datafreshnessstatus import DataFreshnessStatus

class TestDataFreshnessStatus:
    email_users_result = list()

    class TestUser:
        @staticmethod
        def email_users(users_to_email, subject, output):
            TestDataFreshnessStatus.email_users_result.append((users_to_email, subject, output))

    @pytest.fixture(scope='function')
    def database(self):
        dbfile = 'test_freshness.db'
        dbpath = join('tests', dbfile)
        try:
            os.remove(dbpath)
        except FileNotFoundError:
            pass
        shutil.copyfile(join('tests', 'fixtures', dbfile), dbpath)
        return 'sqlite:///%s' % dbpath

    def test_freshnessstatus(self, configuration, database):
        site_url = 'http://lala/'
        users = [{'email': 'blah@blah.com', 'name': 'blah', 'sysadmin': False, 'fullname': 'blah', 'display_name': 'blah'},
                 {'email': 'blah2@blah.com', 'name': 'blah2', 'sysadmin': True, 'fullname': 'blah2', 'display_name': 'blah2'},
                 {'email': 'blah3@blah.com', 'name': 'blah3', 'sysadmin': True, 'fullname': 'blah3', 'display_name': 'blah3'},
                 {'email': 'blah4@blah.com', 'name': 'blah4', 'sysadmin': True, 'fullname': 'blah4', 'display_name': 'blah4'},
                 {'email': 'blah5@blah.com', 'name': 'blah5', 'sysadmin': False, 'fullname': 'blah5', 'display_name': 'blah5'},
                 ]

        ignore_sysadmin_emails = ['blah3@blah.com']
        freshness = DataFreshnessStatus(db_url=database, users=users, ignore_sysadmin_emails=ignore_sysadmin_emails)
        def get_maintainer(dataset):
            if dataset['id'] == 'a2d564de-b2cf-4f93-984f-43e333c9d8cd':
                return users[0]
            if dataset['id'] == 'ca7580d3-2a20-4139-bdc1-dd7ad0b79cc0':
                return users[4]
            return None

        def get_org_admins(dataset):
            if dataset['id'] == '6b2656e2-b915-4671-bfed-468d5edcd80a':
                return [users[3], users[4]]
            if dataset['id'] == '34cb4297-36e2-40b0-b822-47320ea9314c':
                return [users[2], users[3], users[4]]
            return list()

        freshness.get_maintainer = get_maintainer
        freshness.get_org_admins = get_org_admins
        TestDataFreshnessStatus.email_users_result = list()
        freshness.send_delinquent_email(site_url=site_url, userclass=TestDataFreshnessStatus.TestUser)
        assert TestDataFreshnessStatus.email_users_result == \
               [([{'fullname': 'blah2', 'sysadmin': True, 'display_name': 'blah2', 'email': 'blah2@blah.com', 'name': 'blah2'},
                  {'fullname': 'blah4', 'sysadmin': True, 'display_name': 'blah4', 'email': 'blah4@blah.com', 'name': 'blah4'}],
                 'Delinquent datasets', 'Dear system administrator,\n\nThe following datasets have just become delinquent:\n\nAirports in Mayotte (http://lala/dataset/ourairports-myt) from OurAirports maintained by blah5 (blah5@blah.com) with update frequency: Every year\nAirports in Samoa (http://lala/dataset/ourairports-wsm) from OurAirports with missing maintainer and organization administrators blah3 (blah3@blah.com),blah4 (blah4@blah.com),blah5 (blah5@blah.com) with update frequency: Every year\n')]
        TestDataFreshnessStatus.email_users_result = list()

        freshness.send_overdue_emails(site_url=site_url, userclass=TestDataFreshnessStatus.TestUser)
        assert TestDataFreshnessStatus.email_users_result == \
               [([{'sysadmin': False, 'email': 'blah@blah.com', 'display_name': 'blah', 'name': 'blah', 'fullname': 'blah'}],
                 'Overdue datasets', 'Dear blah,\n\nThe following dataset is now overdue for update:\n\nProjected IPC population Estimates February - June 2016 (http://lala/dataset/projected-ipc-population-estimates-february-june-2016) from OCHA Somalia maintained by blah (blah@blah.com) with update frequency: Every six months\n'),
                ([{'sysadmin': True, 'email': 'blah4@blah.com', 'display_name': 'blah4', 'name': 'blah4', 'fullname': 'blah4'},
                  {'sysadmin': False, 'email': 'blah5@blah.com', 'display_name': 'blah5', 'name': 'blah5', 'fullname': 'blah5'}],
                 'Overdue datasets', 'Dear organization administrator,\n\nThe following dataset is now overdue for update:\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) from OCHA Yemen with missing maintainer and organization administrators blah4 (blah4@blah.com),blah5 (blah5@blah.com) with update frequency: Every year\n')]
        freshness.close()
