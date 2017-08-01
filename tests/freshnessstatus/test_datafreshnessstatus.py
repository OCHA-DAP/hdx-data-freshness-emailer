# -*- coding: utf-8 -*-
'''
Unit tests for the data freshness status code.

'''
import os
import shutil
from os.path import join

import pytest
from hdx.data.user import User

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
        users = [{'email': 'blah@blah.com', 'name': 'blah', 'sysadmin': False, 'fullname': 'blahfull', 'display_name': 'blahdisp'},
                 {'email': 'blah2@blah.com', 'name': 'blah2', 'sysadmin': True, 'fullname': 'blah2full'},
                 {'email': 'blah3@blah.com', 'name': 'blah3', 'sysadmin': True, 'fullname': 'blah3full', 'display_name': 'blah3disp'},
                 {'email': 'blah4@blah.com', 'name': 'blah4', 'sysadmin': True, 'fullname': 'blah4full', 'display_name': 'blah4disp'},
                 {'email': 'blah5@blah.com', 'name': 'blah5', 'sysadmin': False, 'fullname': 'blah5full'},
                 ]

        ignore_sysadmin_emails = ['blah3@blah.com']
        freshness = DataFreshnessStatus(db_url=database, users=users, ignore_sysadmin_emails=ignore_sysadmin_emails)
        freshness.orgadmins = {'cdcb3c1f-b8d5-4154-a356-c7021bb1ffbd': [users[3], users[4]],
                               'dc65f72e-ba98-40aa-ad32-bec0ed1e10a2': [users[2], users[3], users[4]]}
        TestDataFreshnessStatus.email_users_result = list()
        freshness.send_delinquent_email(site_url=site_url, userclass=TestDataFreshnessStatus.TestUser)
        assert TestDataFreshnessStatus.email_users_result == \
            [([{'fullname': 'blah2full', 'sysadmin': True, 'email': 'blah2@blah.com', 'name': 'blah2'},
               {'fullname': 'blah4full', 'sysadmin': True, 'display_name': 'blah4disp', 'email': 'blah4@blah.com', 'name': 'blah4'}],
              'Delinquent datasets', 'Dear system administrator,\n\nThe following datasets have just become delinquent:\n\nAirports in Mayotte (http://lala/dataset/ourairports-myt) from OurAirports maintained by blah5full (blah5@blah.com) with update frequency: Every year\nAirports in Samoa (http://lala/dataset/ourairports-wsm) from OurAirports with missing maintainer and organization administrators blah3disp (blah3@blah.com),blah4disp (blah4@blah.com),blah5full (blah5@blah.com) with update frequency: Every year\n')]

        TestDataFreshnessStatus.email_users_result = list()
        freshness.send_overdue_emails(site_url=site_url, userclass=TestDataFreshnessStatus.TestUser)
        assert TestDataFreshnessStatus.email_users_result == \
            [([{'sysadmin': False, 'email': 'blah@blah.com', 'display_name': 'blahdisp', 'name': 'blah', 'fullname': 'blahfull'}],
              'Overdue datasets', 'Dear blahdisp,\n\nThe following dataset is now overdue for update:\n\nProjected IPC population Estimates February - June 2016 (http://lala/dataset/projected-ipc-population-estimates-february-june-2016) from OCHA Somalia maintained by blahdisp (blah@blah.com) with update frequency: Every six months\n'),
             ([{'sysadmin': True, 'email': 'blah4@blah.com', 'display_name': 'blah4disp', 'name': 'blah4', 'fullname': 'blah4full'},
               {'sysadmin': False, 'email': 'blah5@blah.com', 'name': 'blah5', 'fullname': 'blah5full'}],
              'Overdue datasets', 'Dear organization administrator,\n\nThe following dataset is now overdue for update:\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) from OCHA Yemen with missing maintainer and organization administrators blah4disp (blah4@blah.com),blah5full (blah5@blah.com) with update frequency: Every year\n')]
        TestDataFreshnessStatus.email_users_result = list()
        user0 = User(users[0])
        user1 = User(users[1])
        freshness.send_overdue_emails(site_url=site_url, userclass=TestDataFreshnessStatus.TestUser, sendto=[user0, user1])
        assert TestDataFreshnessStatus.email_users_result == \
            [([{'sysadmin': False, 'email': 'blah@blah.com', 'display_name': 'blahdisp', 'name': 'blah', 'fullname': 'blahfull'},
               {'sysadmin': True, 'email': 'blah2@blah.com', 'name': 'blah2', 'fullname': 'blah2full'}],
              'Overdue datasets', 'Dear blahdisp,\n\nThe following dataset is now overdue for update:\n\nProjected IPC population Estimates February - June 2016 (http://lala/dataset/projected-ipc-population-estimates-february-june-2016) from OCHA Somalia maintained by blahdisp (blah@blah.com) with update frequency: Every six months\n'),
             ([{'sysadmin': False, 'email': 'blah@blah.com', 'display_name': 'blahdisp', 'name': 'blah', 'fullname': 'blahfull'},
               {'sysadmin': True, 'email': 'blah2@blah.com', 'name': 'blah2', 'fullname': 'blah2full'}],
              'Overdue datasets', 'Dear organization administrator,\n\nThe following dataset is now overdue for update:\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) from OCHA Yemen with missing maintainer and organization administrators blah4disp (blah4@blah.com),blah5full (blah5@blah.com) with update frequency: Every year\n')]
        freshness.close()
