# -*- coding: utf-8 -*-
'''
Unit tests for the data freshness status code.

'''
import os
import shutil
from os.path import join

import pytest
from hdx.data.user import User

from hdx.freshness.datafreshnessstatus import DataFreshnessStatus


class TestDataFreshnessStatus:
    email_users_result = list()

    class TestUser:
        @staticmethod
        def email_users(users_to_email, subject, output, html_body):
            TestDataFreshnessStatus.email_users_result.append((users_to_email, subject, output, html_body))

    @pytest.fixture(scope='class')
    def users(self):
        return [{'email': 'blah@blah.com', 'id': 'blah', 'name': 'blahname', 'sysadmin': False, 'fullname': 'blahfull', 'display_name': 'blahdisp'},
                {'email': 'blah2@blah.com', 'id': 'blah2', 'name': 'blah2name', 'sysadmin': True, 'fullname': 'blah2full'},
                {'email': 'blah3@blah.com', 'id': 'blah3', 'name': 'blah3name', 'sysadmin': True, 'fullname': 'blah3full', 'display_name': 'blah3disp'},
                {'email': 'blah4@blah.com', 'id': 'blah4', 'name': 'blah4name', 'sysadmin': True, 'fullname': 'blah4full', 'display_name': 'blah4disp'},
                {'email': 'blah5@blah.com', 'id': 'blah5', 'name': 'blah5name', 'sysadmin': False, 'fullname': 'blah5full'}]

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

    @pytest.fixture(scope='function')
    def databasefailure(self):
        dbfile = 'test_freshness_failure.db'
        dbpath = join('tests', dbfile)
        try:
            os.remove(dbpath)
        except FileNotFoundError:
            pass
        shutil.copyfile(join('tests', 'fixtures', dbfile), dbpath)
        return 'sqlite:///%s' % dbpath

    def test_freshnessstatus(self, configuration, database, users):
        site_url = 'http://lala/'
        ignore_sysadmin_emails = ['blah3@blah.com']
        freshness = DataFreshnessStatus(db_url=database, users=users, ignore_sysadmin_emails=ignore_sysadmin_emails)
        run_numbers = freshness.get_runs()
        TestDataFreshnessStatus.email_users_result = list()
        freshness.check_number_datasets(run_numbers=run_numbers, userclass=TestDataFreshnessStatus.TestUser)
        assert TestDataFreshnessStatus.email_users_result == [([{'fullname': 'blah2full', 'sysadmin': True, 'email': 'blah2@blah.com', 'name': 'blah2name', 'id': 'blah2'},
                                                                {'name': 'blah4name', 'fullname': 'blah4full', 'email': 'blah4@blah.com', 'sysadmin': True, 'id': 'blah4', 'display_name': 'blah4disp'}],
                                                               'WARNING: Fall in datasets on HDX today!', 'Dear system administrator,\n\nThere are 1 (16%) fewer datasets today than yesterday on HDX!\n\nBest wishes,\nHDX Team',
                                                               '<html>\n  <head></head>\n  <body>\n    <span>Dear system administrator,<br><br>There are 1 (16%) fewer datasets today than yesterday on HDX!<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n')]

        freshness.orgadmins = {'cdcb3c1f-b8d5-4154-a356-c7021bb1ffbd': [users[3], users[4]],
                               'dc65f72e-ba98-40aa-ad32-bec0ed1e10a2': [users[2], users[3], users[4]]}
        TestDataFreshnessStatus.email_users_result = list()
        freshness.send_delinquent_email(site_url=site_url, run_numbers=run_numbers, userclass=TestDataFreshnessStatus.TestUser)
        assert TestDataFreshnessStatus.email_users_result == \
               [([{'email': 'blah2@blah.com', 'sysadmin': True, 'id': 'blah2', 'name': 'blah2name',
                   'fullname': 'blah2full'},
                  {'sysadmin': True, 'display_name': 'blah4disp', 'id': 'blah4', 'name': 'blah4name',
                   'email': 'blah4@blah.com', 'fullname': 'blah4full'}], 'Delinquent datasets',
                 'Dear system administrator,\n\nThe following datasets have just become delinquent:\n\nAirports in Mayotte (http://lala/dataset/ourairports-myt) from OurAirports maintained by blah5full (blah5@blah.com) with expected update frequency: every year\nAirports in Samoa (http://lala/dataset/ourairports-wsm) from OurAirports with missing maintainer and organization administrators blah3disp (blah3@blah.com), blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: every year\n\nBest wishes,\nHDX Team',
                 '<html>\n  <head></head>\n  <body>\n    <span>Dear system administrator,<br><br>The following datasets have just become delinquent:<br><br><a href="http://lala/dataset/ourairports-myt">Airports in Mayotte</a> from OurAirports maintained by <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: every year<br><a href="http://lala/dataset/ourairports-wsm">Airports in Samoa</a> from OurAirports with missing maintainer and organization administrators <a href="mailto:blah3@blah.com">blah3disp</a>, <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: every year<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n')]

        TestDataFreshnessStatus.email_users_result = list()
        freshness.send_overdue_emails(site_url=site_url, run_numbers=run_numbers, userclass=TestDataFreshnessStatus.TestUser)
        assert TestDataFreshnessStatus.email_users_result == \
               [([{'sysadmin': False, 'email': 'blah@blah.com', 'id': 'blah', 'name': 'blahname',
                   'display_name': 'blahdisp', 'fullname': 'blahfull'}], 'Time to update your datasets on HDX',
                 'Dear blahdisp,\n\nThe dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). Log into the HDX platform now to update each dataset.\n\nTendencias Humanitarias y Paz - Nov 2012 - Dic 2015 (http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015) with expected update frequency: every six months\nProjected IPC population Estimates February - June 2016 (http://lala/dataset/projected-ipc-population-estimates-february-june-2016) with expected update frequency: every six months\n\nTip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.\n\nBest wishes,\nHDX Team',
                 '<html>\n  <head></head>\n  <body>\n    <span>Dear blahdisp,<br><br>The dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). Log into the HDX platform now to update each dataset.<br><br><a href="http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015">Tendencias Humanitarias y Paz - Nov 2012 - Dic 2015</a> with expected update frequency: every six months<br><a href="http://lala/dataset/projected-ipc-population-estimates-february-june-2016">Projected IPC population Estimates February - June 2016</a> with expected update frequency: every six months<br><br>Tip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n'),
                ([{'sysadmin': True, 'email': 'blah4@blah.com', 'id': 'blah4', 'name': 'blah4name',
                   'display_name': 'blah4disp', 'fullname': 'blah4full'}], 'Time to update your datasets on HDX',
                 'Dear blah4disp,\n\nThe dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). Log into the HDX platform now to update each dataset.\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with expected update frequency: every year\n\nTip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.\n\nBest wishes,\nHDX Team',
                 '<html>\n  <head></head>\n  <body>\n    <span>Dear blah4disp,<br><br>The dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). Log into the HDX platform now to update each dataset.<br><br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with expected update frequency: every year<br><br>Tip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n'),
                ([{'name': 'blah5name', 'sysadmin': False, 'email': 'blah5@blah.com', 'fullname': 'blah5full',
                   'id': 'blah5'}], 'Time to update your datasets on HDX',
                 'Dear blah5full,\n\nThe dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). Log into the HDX platform now to update each dataset.\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with expected update frequency: every year\n\nTip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.\n\nBest wishes,\nHDX Team',
                 '<html>\n  <head></head>\n  <body>\n    <span>Dear blah5full,<br><br>The dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). Log into the HDX platform now to update each dataset.<br><br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with expected update frequency: every year<br><br>Tip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n')]
        TestDataFreshnessStatus.email_users_result = list()
        user0 = User(users[0])
        user1 = User(users[1])
        freshness.send_overdue_emails(site_url=site_url, run_numbers=run_numbers, userclass=TestDataFreshnessStatus.TestUser, sendto=[user0, user1])
        assert TestDataFreshnessStatus.email_users_result == \
               [([{'fullname': 'blahfull', 'sysadmin': False, 'email': 'blah@blah.com', 'id': 'blah',
                   'display_name': 'blahdisp', 'name': 'blahname'},
                  {'fullname': 'blah2full', 'name': 'blah2name', 'email': 'blah2@blah.com', 'sysadmin': True,
                   'id': 'blah2'}], 'Time to update your datasets on HDX',
                 'Dear blahdisp,\n\nThe dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). Log into the HDX platform now to update each dataset.\n\nTendencias Humanitarias y Paz - Nov 2012 - Dic 2015 (http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015) with expected update frequency: every six months\nProjected IPC population Estimates February - June 2016 (http://lala/dataset/projected-ipc-population-estimates-february-june-2016) with expected update frequency: every six months\n\nTip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.\n\nBest wishes,\nHDX Team',
                 '<html>\n  <head></head>\n  <body>\n    <span>Dear blahdisp,<br><br>The dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). Log into the HDX platform now to update each dataset.<br><br><a href="http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015">Tendencias Humanitarias y Paz - Nov 2012 - Dic 2015</a> with expected update frequency: every six months<br><a href="http://lala/dataset/projected-ipc-population-estimates-february-june-2016">Projected IPC population Estimates February - June 2016</a> with expected update frequency: every six months<br><br>Tip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n'),
                ([{'fullname': 'blahfull', 'sysadmin': False, 'email': 'blah@blah.com', 'id': 'blah',
                   'display_name': 'blahdisp', 'name': 'blahname'},
                  {'fullname': 'blah2full', 'name': 'blah2name', 'email': 'blah2@blah.com', 'sysadmin': True,
                   'id': 'blah2'}], 'Time to update your datasets on HDX',
                 'Dear blah4disp,\n\nThe dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). Log into the HDX platform now to update each dataset.\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with expected update frequency: every year\n\nTip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.\n\nBest wishes,\nHDX Team',
                 '<html>\n  <head></head>\n  <body>\n    <span>Dear blah4disp,<br><br>The dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). Log into the HDX platform now to update each dataset.<br><br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with expected update frequency: every year<br><br>Tip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n'),
                ([{'fullname': 'blahfull', 'sysadmin': False, 'email': 'blah@blah.com', 'id': 'blah',
                   'display_name': 'blahdisp', 'name': 'blahname'},
                  {'fullname': 'blah2full', 'name': 'blah2name', 'email': 'blah2@blah.com', 'sysadmin': True,
                   'id': 'blah2'}], 'Time to update your datasets on HDX',
                 'Dear blah5full,\n\nThe dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). Log into the HDX platform now to update each dataset.\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with expected update frequency: every year\n\nTip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.\n\nBest wishes,\nHDX Team',
                 '<html>\n  <head></head>\n  <body>\n    <span>Dear blah5full,<br><br>The dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). Log into the HDX platform now to update each dataset.<br><br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with expected update frequency: every year<br><br>Tip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n')]
        freshness.close()

    def test_freshnessfailure(self, configuration, databasefailure, users):
        freshness = DataFreshnessStatus(db_url=databasefailure, users=users)
        run_numbers = freshness.get_runs()
        TestDataFreshnessStatus.email_users_result = list()
        mikeuser = User({'email': 'mcarans@yahoo.co.uk', 'name': 'mcarans', 'sysadmin': True, 'fullname': 'Michael Rans', 'display_name': 'Michael Rans'})
        serbanuser = User({'email': 'teodorescu.serban@gmail.com', 'name': 'serban', 'sysadmin': True, 'fullname': 'Serban Teodorescu', 'display_name': 'Serban Teodorescu'})
        freshness.check_number_datasets(run_numbers=run_numbers, send_failures=[mikeuser, serbanuser], userclass=TestDataFreshnessStatus.TestUser)
        assert TestDataFreshnessStatus.email_users_result == [([{'display_name': 'Michael Rans', 'sysadmin': True, 'name': 'mcarans', 'fullname': 'Michael Rans', 'email': 'mcarans@yahoo.co.uk'}, {'display_name': 'Serban Teodorescu', 'sysadmin': True, 'name': 'serban', 'fullname': 'Serban Teodorescu', 'email': 'teodorescu.serban@gmail.com'}],
                                                               'FAILURE: No datasets today!', 'Dear system administrator,\n\nIt is highly probable that data freshness has failed!\n\nBest wishes,\nHDX Team', '<html>\n  <head></head>\n  <body>\n    <span>Dear system administrator,<br><br>It is highly probable that data freshness has failed!<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n')]