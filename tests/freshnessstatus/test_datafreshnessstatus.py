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
        def email_users(users_to_email, subject, output, html_body):
            TestDataFreshnessStatus.email_users_result.append((users_to_email, subject, output, html_body))

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
        assert TestDataFreshnessStatus.email_users_result == [([{'fullname': 'blah2full', 'sysadmin': True, 'email': 'blah2@blah.com', 'name': 'blah2name', 'id': 'blah2'},
                                                                {'name': 'blah4name', 'fullname': 'blah4full', 'email': 'blah4@blah.com', 'sysadmin': True, 'id': 'blah4', 'display_name': 'blah4disp'}],
                                                               'Fewer datasets today!', 'Dear system administrator,\n\nThere are 1 (16%) fewer datasets today than yesterday on HDX!\n\nBest wishes,\nHDX Team',
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
                 'Dear system administrator,\n\nThe following datasets have just become delinquent:\n\nAirports in Mayotte (http://lala/dataset/ourairports-myt) from OurAirports maintained by blah5full (blah5@blah.com) with update frequency: Every year\nAirports in Samoa (http://lala/dataset/ourairports-wsm) from OurAirports with missing maintainer and organization administrators blah3disp (blah3@blah.com), blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with update frequency: Every year\n\nBest wishes,\nHDX Team',
                 '<html>\n  <head></head>\n  <body>\n    <span>Dear system administrator,<br><br>The following datasets have just become delinquent:<br><br><a href="http://lala/dataset/ourairports-myt">Airports in Mayotte</a> from OurAirports maintained by <a href="mailto:blah5@blah.com">blah5full</a> with update frequency: Every year<br><a href="http://lala/dataset/ourairports-wsm">Airports in Samoa</a> from OurAirports with missing maintainer and organization administrators <a href="mailto:blah3@blah.com">blah3disp</a>, <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with update frequency: Every year<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n')]

        TestDataFreshnessStatus.email_users_result = list()
        freshness.send_overdue_emails(site_url=site_url, run_numbers=run_numbers, userclass=TestDataFreshnessStatus.TestUser)
        assert TestDataFreshnessStatus.email_users_result == \
               [([{'sysadmin': False, 'email': 'blah@blah.com', 'id': 'blah', 'name': 'blahname',
                   'display_name': 'blahdisp', 'fullname': 'blahfull'}], 'Overdue datasets',
                 'Dear blahdisp,\n\nThe following datasets are now overdue for update:\n\nTendencias Humanitarias y Paz - Nov 2012 - Dic 2015 (http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015) from OCHA Colombia maintained by blahdisp (blah@blah.com) with update frequency: Every six months\nProjected IPC population Estimates February - June 2016 (http://lala/dataset/projected-ipc-population-estimates-february-june-2016) from OCHA Somalia maintained by blahdisp (blah@blah.com) with update frequency: Every six months\n\nBest wishes,\nHDX Team',
                 '<html>\n  <head></head>\n  <body>\n    <span>Dear blahdisp,<br><br>The following datasets are now overdue for update:<br><br><a href="http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015">Tendencias Humanitarias y Paz - Nov 2012 - Dic 2015</a> from OCHA Colombia maintained by <a href="mailto:blah@blah.com">blahdisp</a> with update frequency: Every six months<br><a href="http://lala/dataset/projected-ipc-population-estimates-february-june-2016">Projected IPC population Estimates February - June 2016</a> from OCHA Somalia maintained by <a href="mailto:blah@blah.com">blahdisp</a> with update frequency: Every six months<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n'),
                ([{'sysadmin': True, 'email': 'blah4@blah.com', 'id': 'blah4', 'name': 'blah4name',
                   'display_name': 'blah4disp', 'fullname': 'blah4full'}], 'Overdue datasets',
                 'Dear blah4disp,\n\nThe following datasets are now overdue for update:\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) from OCHA Yemen with missing maintainer and organization administrators blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with update frequency: Every year\n\nBest wishes,\nHDX Team',
                 '<html>\n  <head></head>\n  <body>\n    <span>Dear blah4disp,<br><br>The following datasets are now overdue for update:<br><br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> from OCHA Yemen with missing maintainer and organization administrators <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with update frequency: Every year<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n'),
                ([{'name': 'blah5name', 'sysadmin': False, 'email': 'blah5@blah.com', 'fullname': 'blah5full',
                   'id': 'blah5'}], 'Overdue datasets',
                 'Dear blah5full,\n\nThe following datasets are now overdue for update:\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) from OCHA Yemen with missing maintainer and organization administrators blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with update frequency: Every year\n\nBest wishes,\nHDX Team',
                 '<html>\n  <head></head>\n  <body>\n    <span>Dear blah5full,<br><br>The following datasets are now overdue for update:<br><br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> from OCHA Yemen with missing maintainer and organization administrators <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with update frequency: Every year<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n')]

        TestDataFreshnessStatus.email_users_result = list()
        user0 = User(users[0])
        user1 = User(users[1])
        freshness.send_overdue_emails(site_url=site_url, run_numbers=run_numbers, userclass=TestDataFreshnessStatus.TestUser, sendto=[user0, user1])
        assert TestDataFreshnessStatus.email_users_result == \
               [([{'fullname': 'blahfull', 'sysadmin': False, 'email': 'blah@blah.com', 'id': 'blah',
                   'display_name': 'blahdisp', 'name': 'blahname'},
                  {'fullname': 'blah2full', 'name': 'blah2name', 'email': 'blah2@blah.com', 'sysadmin': True,
                   'id': 'blah2'}], 'Overdue datasets',
                 'Dear blahdisp,\n\nThe following datasets are now overdue for update:\n\nTendencias Humanitarias y Paz - Nov 2012 - Dic 2015 (http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015) from OCHA Colombia maintained by blahdisp (blah@blah.com) with update frequency: Every six months\nProjected IPC population Estimates February - June 2016 (http://lala/dataset/projected-ipc-population-estimates-february-june-2016) from OCHA Somalia maintained by blahdisp (blah@blah.com) with update frequency: Every six months\n\nBest wishes,\nHDX Team',
                 '<html>\n  <head></head>\n  <body>\n    <span>Dear blahdisp,<br><br>The following datasets are now overdue for update:<br><br><a href="http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015">Tendencias Humanitarias y Paz - Nov 2012 - Dic 2015</a> from OCHA Colombia maintained by <a href="mailto:blah@blah.com">blahdisp</a> with update frequency: Every six months<br><a href="http://lala/dataset/projected-ipc-population-estimates-february-june-2016">Projected IPC population Estimates February - June 2016</a> from OCHA Somalia maintained by <a href="mailto:blah@blah.com">blahdisp</a> with update frequency: Every six months<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n'),
                ([{'fullname': 'blahfull', 'sysadmin': False, 'email': 'blah@blah.com', 'id': 'blah',
                   'display_name': 'blahdisp', 'name': 'blahname'},
                  {'fullname': 'blah2full', 'name': 'blah2name', 'email': 'blah2@blah.com', 'sysadmin': True,
                   'id': 'blah2'}], 'Overdue datasets',
                 'Dear blah4disp,\n\nThe following datasets are now overdue for update:\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) from OCHA Yemen with missing maintainer and organization administrators blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with update frequency: Every year\n\nBest wishes,\nHDX Team',
                 '<html>\n  <head></head>\n  <body>\n    <span>Dear blah4disp,<br><br>The following datasets are now overdue for update:<br><br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> from OCHA Yemen with missing maintainer and organization administrators <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with update frequency: Every year<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n'),
                ([{'fullname': 'blahfull', 'sysadmin': False, 'email': 'blah@blah.com', 'id': 'blah',
                   'display_name': 'blahdisp', 'name': 'blahname'},
                  {'fullname': 'blah2full', 'name': 'blah2name', 'email': 'blah2@blah.com', 'sysadmin': True,
                   'id': 'blah2'}], 'Overdue datasets',
                 'Dear blah5full,\n\nThe following datasets are now overdue for update:\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) from OCHA Yemen with missing maintainer and organization administrators blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with update frequency: Every year\n\nBest wishes,\nHDX Team',
                 '<html>\n  <head></head>\n  <body>\n    <span>Dear blah5full,<br><br>The following datasets are now overdue for update:<br><br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> from OCHA Yemen with missing maintainer and organization administrators <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with update frequency: Every year<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n')]
        freshness.close()
