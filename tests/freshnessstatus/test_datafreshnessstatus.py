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
        users = [{'email': 'blah@blah.com', 'id': 'blah', 'name': 'blahname', 'sysadmin': False, 'fullname': 'blahfull', 'display_name': 'blahdisp'},
                 {'email': 'blah2@blah.com', 'id': 'blah2', 'name': 'blah2name', 'sysadmin': True, 'fullname': 'blah2full'},
                 {'email': 'blah3@blah.com', 'id': 'blah3', 'name': 'blah3name', 'sysadmin': True, 'fullname': 'blah3full', 'display_name': 'blah3disp'},
                 {'email': 'blah4@blah.com', 'id': 'blah4', 'name': 'blah4name', 'sysadmin': True, 'fullname': 'blah4full', 'display_name': 'blah4disp'},
                 {'email': 'blah5@blah.com', 'id': 'blah5', 'name': 'blah5name', 'sysadmin': False, 'fullname': 'blah5full'},
                 ]

        ignore_sysadmin_emails = ['blah3@blah.com']
        freshness = DataFreshnessStatus(db_url=database, users=users, ignore_sysadmin_emails=ignore_sysadmin_emails)
        run_numbers = freshness.get_runs()
        TestDataFreshnessStatus.email_users_result = list()
        freshness.check_number_datasets(run_numbers=run_numbers, userclass=TestDataFreshnessStatus.TestUser)
        assert TestDataFreshnessStatus.email_users_result == [([{'id': 'blah2', 'sysadmin': True, 'email': 'blah2@blah.com', 'name': 'blah2name', 'fullname': 'blah2full'},
                                                                {'id': 'blah4', 'sysadmin': True, 'fullname': 'blah4full', 'name': 'blah4name', 'email': 'blah4@blah.com', 'display_name': 'blah4disp'}],
                                                               'Fewer datasets today!', 'Dear system administrator,\n\nThere are 1 (16%) fewer datasets today than yesterday on HDX!')]
        freshness.orgadmins = {'cdcb3c1f-b8d5-4154-a356-c7021bb1ffbd': [users[3], users[4]],
                               'dc65f72e-ba98-40aa-ad32-bec0ed1e10a2': [users[2], users[3], users[4]]}
        TestDataFreshnessStatus.email_users_result = list()
        freshness.send_delinquent_email(site_url=site_url, run_numbers=run_numbers, userclass=TestDataFreshnessStatus.TestUser)
        assert TestDataFreshnessStatus.email_users_result == \
            [([{'fullname': 'blah2full', 'sysadmin': True, 'email': 'blah2@blah.com', 'name': 'blah2name', 'id': 'blah2'},
               {'fullname': 'blah4full', 'sysadmin': True, 'display_name': 'blah4disp', 'email': 'blah4@blah.com', 'name': 'blah4name', 'id': 'blah4'}],
              'Delinquent datasets', 'Dear system administrator,\n\nThe following datasets have just become delinquent:\n\nAirports in Mayotte (http://lala/dataset/ourairports-myt) from OurAirports maintained by blah5full (blah5@blah.com) with update frequency: Every year\nAirports in Samoa (http://lala/dataset/ourairports-wsm) from OurAirports with missing maintainer and organization administrators blah3disp (blah3@blah.com),blah4disp (blah4@blah.com),blah5full (blah5@blah.com) with update frequency: Every year\n')]

        TestDataFreshnessStatus.email_users_result = list()
        freshness.send_overdue_emails(site_url=site_url, run_numbers=run_numbers, userclass=TestDataFreshnessStatus.TestUser)
        assert TestDataFreshnessStatus.email_users_result == \
               [([{'id': 'blah', 'sysadmin': False, 'fullname': 'blahfull', 'name': 'blahname', 'email': 'blah@blah.com',
                   'display_name': 'blahdisp'}], 'Overdue datasets',
                 'Dear blahdisp,\n\nThe following datasets are now overdue for update:\n\nTendencias Humanitarias y Paz - Nov 2012 - Dic 2015 (http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015) from OCHA Colombia maintained by blahdisp (blah@blah.com) with update frequency: Every six months\n'
                 'Projected IPC population Estimates February - June 2016 (http://lala/dataset/projected-ipc-population-estimates-february-june-2016) from OCHA Somalia maintained by blahdisp (blah@blah.com) with update frequency: Every six months\n'),
                ([{'id': 'blah4', 'sysadmin': True, 'fullname': 'blah4full', 'name': 'blah4name',
                   'email': 'blah4@blah.com', 'display_name': 'blah4disp'}], 'Overdue datasets',
                 'Dear blah4disp,\n\nThe following datasets are now overdue for update:\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) from OCHA Yemen with missing maintainer and organization administrators blah4disp (blah4@blah.com),blah5full (blah5@blah.com) with update frequency: Every year\n'),
                ([{'id': 'blah5', 'name': 'blah5name', 'email': 'blah5@blah.com', 'sysadmin': False,
                   'fullname': 'blah5full'}], 'Overdue datasets',
                 'Dear blah5full,\n\nThe following datasets are now overdue for update:\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) from OCHA Yemen with missing maintainer and organization administrators blah4disp (blah4@blah.com),blah5full (blah5@blah.com) with update frequency: Every year\n')]

        TestDataFreshnessStatus.email_users_result = list()
        user0 = User(users[0])
        user1 = User(users[1])
        freshness.send_overdue_emails(site_url=site_url, run_numbers=run_numbers, userclass=TestDataFreshnessStatus.TestUser, sendto=[user0, user1])
        assert TestDataFreshnessStatus.email_users_result == \
               [([{'name': 'blahname', 'id': 'blah', 'email': 'blah@blah.com', 'display_name': 'blahdisp',
                   'sysadmin': False, 'fullname': 'blahfull'},
                  {'email': 'blah2@blah.com', 'name': 'blah2name', 'sysadmin': True, 'id': 'blah2',
                   'fullname': 'blah2full'}], 'Overdue datasets',
                 'Dear blahdisp,\n\nThe following datasets are now overdue for update:\n\nTendencias Humanitarias y Paz - Nov 2012 - Dic 2015 (http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015) from OCHA Colombia maintained by blahdisp (blah@blah.com) with update frequency: Every six months\n'
                 'Projected IPC population Estimates February - June 2016 (http://lala/dataset/projected-ipc-population-estimates-february-june-2016) from OCHA Somalia maintained by blahdisp (blah@blah.com) with update frequency: Every six months\n'),
                ([{'name': 'blahname', 'id': 'blah', 'email': 'blah@blah.com', 'display_name': 'blahdisp',
                   'sysadmin': False, 'fullname': 'blahfull'},
                  {'email': 'blah2@blah.com', 'name': 'blah2name', 'sysadmin': True, 'id': 'blah2',
                   'fullname': 'blah2full'}], 'Overdue datasets',
                 'Dear blah4disp,\n\nThe following datasets are now overdue for update:\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) from OCHA Yemen with missing maintainer and organization administrators blah4disp (blah4@blah.com),blah5full (blah5@blah.com) with update frequency: Every year\n'),
                ([{'name': 'blahname', 'id': 'blah', 'email': 'blah@blah.com', 'display_name': 'blahdisp',
                   'sysadmin': False, 'fullname': 'blahfull'},
                  {'email': 'blah2@blah.com', 'name': 'blah2name', 'sysadmin': True, 'id': 'blah2',
                   'fullname': 'blah2full'}], 'Overdue datasets',
                 'Dear blah5full,\n\nThe following datasets are now overdue for update:\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) from OCHA Yemen with missing maintainer and organization administrators blah4disp (blah4@blah.com),blah5full (blah5@blah.com) with update frequency: Every year\n')]
        freshness.close()
