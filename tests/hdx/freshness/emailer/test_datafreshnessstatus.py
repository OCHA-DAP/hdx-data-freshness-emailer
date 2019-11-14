# -*- coding: utf-8 -*-
'''
Unit tests for the data freshness status code.

'''
import copy
import os
import shutil
from os.path import join

import pytest
from dateutil import parser
from hdx.data.user import User
from hdx.hdx_configuration import Configuration
from hdx.utilities.database import Database

from hdx.freshness.emailer.databasequeries import DatabaseQueries
from hdx.freshness.emailer.datafreshnessstatus import DataFreshnessStatus
from hdx.freshness.emailer.datasethelper import DatasetHelper
from hdx.freshness.emailer.freshnessemail import Email
from hdx.freshness.emailer.sheet import Sheet


class TestDataFreshnessStatus:
    email_users_result = list()
    cells_result = list()

    @staticmethod
    def email_users(users_to_email, subject, output, html_body):
        TestDataFreshnessStatus.email_users_result.append((users_to_email, subject, output, html_body))

    class TestDataset:
        @staticmethod
        def search_in_hdx(fq):
            if 'airport' in fq:
                return [{'id': '34cb4297-36e2-40b0-b822-47320ea9314c'}]
            else:
                return list()

    class TestSpreadsheet_Broken1:
        @staticmethod
        def worksheet_by_title(_):
            class TestWorksheet:
                @staticmethod
                def get_all_values(returnas):
                    return [['URL', 'Title', 'Organisation', 'Maintainer', 'Maintainer Email', 'Org Admins',
                             'Org Admin Emails', 'Update Frequency', 'Latest of Modifieds', 'Freshness', 'Error Type',
                             'Error', 'Date Added', 'No. Times', 'Assigned', 'Status'],
                            ['http://lala/dataset/yemen-admin-boundaries', 'Yemen - Administrative Boundaries',
                             'OCHA Yemen', '', '', 'blah4disp,blah5full', 'blah4@blah.com,blah5@blah.com', 'every year',
                             '2015-12-28T06:39:20.134647', 'Delinquent', 'Server Error (may be temporary)',
                             'Admin-0.zip:Fail\nAdmin-3.zip:Fail', '2017-01-02T19:07:30.333492', 3, 'Andrew',
                             'Contacted Maintainer']]

                @staticmethod
                def update_values(_, cells):
                    TestDataFreshnessStatus.cells_result = cells
            return TestWorksheet

    class TestSpreadsheet_Broken2:
        @staticmethod
        def worksheet_by_title(_):
            class TestWorksheet:
                @staticmethod
                def get_all_values(returnas):
                    return [['URL', 'Title', 'Organisation', 'Maintainer', 'Maintainer Email', 'Org Admins',
                             'Org Admin Emails', 'Update Frequency', 'Latest of Modifieds', 'Freshness', 'Error Type',
                             'Error', 'Date Added', 'No. Times', 'Assigned', 'Status']]

                @staticmethod
                def update_values(_, cells):
                    TestDataFreshnessStatus.cells_result = cells
            return TestWorksheet

    class TestSpreadsheet_OverdueDelinquent:
        @staticmethod
        def worksheet_by_title(_):
            class TestWorksheet:
                @staticmethod
                def get_all_values(returnas):
                    return [['URL', 'Title', 'Organisation', 'Maintainer', 'Maintainer Email', 'Org Admins',
                             'Org Admin Emails', 'Update Frequency', 'Latest of Modifieds', 'Date Added', 'No. Times',
                             'Assigned', 'Status'],
                            ['http://lala/dataset/ourairports-myt', 'Airports in Mayotte', 'OurAirports', 'blah5full',
                             'blah5@blah.com', 'blah3disp,blah4disp,blah5full',
                             'blah3@blah.com,blah4@blah.com,blah5@blah.com', 'every year', '2015-11-24T23:32:32.025059',
                             '2017-01-01T19:07:30.333492', 2, 'Peter', 'Done']]

                @staticmethod
                def update_values(_, cells):
                    TestDataFreshnessStatus.cells_result = cells
            return TestWorksheet

    class TestSpreadsheet_MaintainerOrgAdmins:
        @staticmethod
        def worksheet_by_title(sheetname):
            class TestWorksheet:

                @staticmethod
                def get_all_values(returnas):
                    if TestWorksheet.sheetname == 'Maintainer':
                        return [['URL', 'Title', 'Organisation', 'Maintainer', 'Maintainer Email', 'Org Admins',
                                 'Org Admin Emails', 'Update Frequency', 'Latest of Modifieds', 'Date Added',
                                 'No. Times',
                                 'Assigned', 'Status'],
                                ['http://lala/dataset/ourairports-myt', 'Airports in Mayotte', 'OurAirports',
                                 'blah5full',
                                 'blah5@blah.com', 'blah3disp,blah4disp,blah5full',
                                 'blah3@blah.com,blah4@blah.com,blah5@blah.com', 'every year',
                                 '2015-11-24T23:32:32.025059',
                                 '2017-01-01T19:07:30.333492', 3, 'Aaron', 'Done']]
                    else:
                        return [['URL', 'Title', 'Error', 'Date Added', 'No. Times', 'Assigned', 'Status'],
                                ['http://lala/organization/ocha-somalia', 'OCHA Somalia',
                                 'All org admins are sysadmins!', '2017-01-01T19:07:30.333492', 5, 'Aaron', '']]

                @staticmethod
                def update_values(_, cells):
                    TestDataFreshnessStatus.cells_result.append(cells)

            TestWorksheet.sheetname = sheetname
            return TestWorksheet

    class TestSpreadsheet_NoResources:
        @staticmethod
        def worksheet_by_title(_):
            class TestWorksheet:
                @staticmethod
                def get_all_values(returnas):
                    return [['URL', 'Title', 'Organisation', 'Maintainer', 'Maintainer Email', 'Org Admins',
                             'Org Admin Emails', 'Update Frequency', 'Latest of Modifieds', 'Date Added', 'No. Times',
                             'Assigned', 'Status'],
                            ['http://lala/dataset/ourairports-myt', 'Airports in Mayotte', 'OurAirports', 'blah5full',
                             'blah5@blah.com', 'blah3disp,blah4disp,blah5full',
                             'blah3@blah.com,blah4@blah.com,blah5@blah.com', 'every year', '2015-11-24T23:32:32.025059',
                             '2017-01-01T19:07:30.333492', 2, 'Peter', 'Done']]

                @staticmethod
                def update_values(_, cells):
                    TestDataFreshnessStatus.cells_result = cells

            return TestWorksheet

    class TestSpreadsheet_Empty:
        @staticmethod
        def worksheet_by_title(_):
            class TestWorksheet:
                @staticmethod
                def get_all_values(returnas):
                    return [['URL', 'Title', 'Organisation', 'Maintainer', 'Maintainer Email', 'Org Admins',
                             'Org Admin Emails', 'Dataset Date', 'Update Frequency', 'Latest of Modifieds',
                             'Date Added',
                             'No. Times', 'Assigned', 'Status']]

                @staticmethod
                def update_values(_, cells):
                    TestDataFreshnessStatus.cells_result = cells

            return TestWorksheet

    @pytest.fixture(scope='class')
    def users(self):
        return [{'email': 'blah@blah.com', 'id': 'blah', 'name': 'blahname', 'sysadmin': False, 'fullname': 'blahfull', 'display_name': 'blahdisp'},
                {'email': 'blah2@blah.com', 'id': 'blah2', 'name': 'blah2name', 'sysadmin': True, 'fullname': 'blah2full'},
                {'email': 'blah3@blah.com', 'id': 'blah3', 'name': 'blah3name', 'sysadmin': True, 'fullname': 'blah3full', 'display_name': 'blah3disp'},
                {'email': 'blah4@blah.com', 'id': 'blah4', 'name': 'blah4name', 'sysadmin': True, 'fullname': 'blah4full', 'display_name': 'blah4disp'},
                {'email': 'blah5@blah.com', 'id': 'blah5', 'name': 'blah5name', 'sysadmin': False, 'fullname': 'blah5full'}]

    @pytest.fixture(scope='class')
    def organizations(self, users):
        orgusers = list()
        for user in users:
            orguser = {'capacity': 'admin'}
            orguser.update(user)
            del orguser['email']
            orgusers.append(orguser)
        return [
            {'display_name': 'OCHA Colombia', 'description': 'OCHA Colombia', 'image_display_url': '',
             'package_count': 147, 'created': '2014-04-28T17:50:16.250998', 'name': 'ocha-colombia',
             'is_organization': True, 'state': 'active', 'image_url': '', 'type': 'organization',
             'title': 'OCHA Colombia', 'revision_id': '7b70966b-c614-47e2-99d7-fafce4cbd2fa', 'num_followers': 0,
             'id': '15942bd7-524a-40d6-8a60-09bd78110d2d', 'approval_status': 'approved',
             'users': [copy.deepcopy(orgusers[2]), copy.deepcopy(orgusers[4])]},
            {'display_name': 'OCHA Somalia', 'description': 'OCHA Somalia', 'image_display_url': '',
             'package_count': 27, 'created': '2014-11-06T17:35:37.390084', 'name': 'ocha-somalia',
             'is_organization': True, 'state': 'active', 'image_url': '', 'type': 'organization',
             'title': 'OCHA Somalia', 'revision_id': '6eb690cc-7821-45e1-99a0-6094894a04d7', 'num_followers': 0,
             'id': '68aa2b4d-ea41-4b79-8e37-ac03cbe9ddca', 'approval_status': 'approved',
             'users': [copy.deepcopy(orgusers[2]), copy.deepcopy(orgusers[3])]},
            {'display_name': 'OCHA Yemen', 'description': 'OCHA Yemen.', 'image_display_url': '', 'package_count': 40,
             'created': '2014-04-28T17:47:48.530487', 'name': 'ocha-yemen', 'is_organization': True, 'state': 'active',
             'image_url': '', 'type': 'organization', 'title': 'OCHA Yemen',
             'revision_id': 'd0f65677-e8ef-46a5-a28f-6c8ab3acf05e', 'num_followers': 0,
             'id': 'cdcb3c1f-b8d5-4154-a356-c7021bb1ffbd', 'approval_status': 'approved',
             'users': [copy.deepcopy(orgusers[3]), copy.deepcopy(orgusers[4])]},
            {'display_name': 'OurAirports', 'description': 'http://ourairports.com', 'image_display_url': '',
             'package_count': 238, 'created': '2014-04-24T22:00:54.948536', 'name': 'ourairports',
             'is_organization': True, 'state': 'active', 'image_url': '', 'type': 'organization',
             'title': 'OurAirports', 'revision_id': '720eae06-2877-4d13-8af4-30061f6a72a5', 'num_followers': 0,
             'id': 'dc65f72e-ba98-40aa-ad32-bec0ed1e10a2', 'approval_status': 'approved',
             'users': [copy.deepcopy(orgusers[2]), copy.deepcopy(orgusers[3]), copy.deepcopy(orgusers[4])]},
        ]

    @pytest.fixture(scope='function')
    def database_broken(self):
        dbfile = 'test_freshness_broken.db'
        dbpath = join('tests', dbfile)
        try:
            os.remove(dbpath)
        except FileNotFoundError:
            pass
        shutil.copyfile(join('tests', 'fixtures', dbfile), dbpath)
        return {'driver': 'sqlite', 'database': dbpath}

    @pytest.fixture(scope='function')
    def database_status(self):
        dbfile = 'test_freshness_status.db'
        dbpath = join('tests', dbfile)
        try:
            os.remove(dbpath)
        except FileNotFoundError:
            pass
        shutil.copyfile(join('tests', 'fixtures', dbfile), dbpath)
        return {'driver': 'sqlite', 'database': dbpath}

    @pytest.fixture(scope='function')
    def database_maintainer(self):
        dbfile = 'test_freshness_maintainer.db'
        dbpath = join('tests', dbfile)
        try:
            os.remove(dbpath)
        except FileNotFoundError:
            pass
        shutil.copyfile(join('tests', 'fixtures', dbfile), dbpath)
        return {'driver': 'sqlite', 'database': dbpath}

    @pytest.fixture(scope='function')
    def database_noresources(self):
        dbfile = 'test_freshness_noresources.db'
        dbpath = join('tests', dbfile)
        try:
            os.remove(dbpath)
        except FileNotFoundError:
            pass
        shutil.copyfile(join('tests', 'fixtures', dbfile), dbpath)
        return {'driver': 'sqlite', 'database': dbpath}

    @pytest.fixture(scope='function')
    def database_datasets_modified_yesterday(self):
        dbfile = 'test_freshness_modified_yesterday.db'
        dbpath = join('tests', dbfile)
        try:
            os.remove(dbpath)
        except FileNotFoundError:
            pass
        shutil.copyfile(join('tests', 'fixtures', dbfile), dbpath)
        return {'driver': 'sqlite', 'database': dbpath}

    def test_freshnessbroken(self, configuration, database_broken, users, organizations):
        site_url = 'http://lala/'
        sysadmins_to_email = ['blah3@blah.com', 'blah4@blah.com']
        now = parser.parse('2017-02-03 19:07:30.333492')
        sheet = Sheet(now)
        email = Email(send_emails=self.email_users)
        with Database(**database_broken) as session:
            datasethelper = DatasetHelper(site_url=site_url, users=users, organizations=organizations,
                                          sysadmins_to_email=sysadmins_to_email)
            databasequeries = DatabaseQueries(session=session, now=now)
            freshness = DataFreshnessStatus(datasethelper=datasethelper, databasequeries=databasequeries, email=email,
                                            sheet=sheet)

            sheet.spreadsheet = TestDataFreshnessStatus.TestSpreadsheet_Broken1
            sheet.dutyofficer = 'Peter'
            TestDataFreshnessStatus.email_users_result = list()
            TestDataFreshnessStatus.cells_result = None
            freshness.process_broken()
            assert TestDataFreshnessStatus.email_users_result == \
                   [(['blah3@blah.com', 'blah4@blah.com'], 'Broken datasets',
                     'Dear system administrator,\n\nThe following datasets have broken resources:\n\nClientConnectorError\nOCHA Somalia\n    Projected IPC population Estimates February - June 2016 (http://lala/dataset/projected-ipc-population-estimates-february-june-2016) maintained by blahdisp (blah@blah.com) with expected update frequency: every six months and freshness: Delinquent\n        Resource Rural-Urban-and-IDP-Projected-Population-February-June-2016[1].xlsx (93a361c6-ace7-4cea-8407-ffd2c30d0853) has error: error: code= message=Cannot connect to host xxx ssl:False [Connection refused] raised=aiohttp.client_exceptions.ClientConnectorError url=xxx!\n\nClientConnectorSSLError\nOCHA Yemen\n    Yemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with missing maintainer and organization administrators blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: every year and freshness: Delinquent\n        Resource Admin-1.zip (69146e1e-62c7-4e7f-8f6c-2dacffe02283) has error: error: code= message=Cannot connect to host xxx ssl:True [[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed (_ssl.c:645)] raised=aiohttp.client_exceptions.ClientConnectorSSLError url=xxx!\n        Resource Admin-2.zip (f60035dc-624a-49cf-95de-9d489c07d3b9) has error: error: code= message=Cannot connect to host xxx ssl:True [[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed (_ssl.c:645)] raised=aiohttp.client_exceptions.ClientConnectorSSLError url=xxx!\n\nClientResponseError\nOCHA Colombia\n    Tendencias Humanitarias y Paz - Nov 2012 - Dic 2015 (http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015) maintained by blahdisp (blah@blah.com) with expected update frequency: every six months and freshness: Delinquent\n        Resource 160304Tendencias_Humanitarias_2016_I.xlsx (256d8b17-5975-4be6-8985-5df18dda061e) has error: code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx!\n        Resource 160304Tendencias_Humanitarias_2016_I_2.xlsx (256d8b17-5975-4be6-8985-5df18dda061a) has error: code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx!\n\nServer Error (may be temporary)\nOCHA Yemen\n    Yemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with missing maintainer and organization administrators blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: every year and freshness: Delinquent\n        Resource Admin-0.zip (2ade2886-2990-41d0-a89b-33c5d1de6e3a) has error: Fail!\n        Resource Admin-3.zip (f60035dc-624a-49cf-95de-9d489c07d3ba) has error: Fail!\nOurAirports\n    Airports in Mayotte (http://lala/dataset/ourairports-myt) maintained by blah5full (blah5@blah.com) with expected update frequency: every year and freshness: Delinquent\n        Resource List of airports in Mayotte (HXL tags) (89a99c0a-4cbf-4717-9cde-987042bc435f) has error: code= message=Connection timeout to host xxx raised=aiohttp.client_exceptions.ServerTimeoutError url=xxx!\n        Resource List of airports in Mayotte (no HXL tags) (a5797320-ce50-4b3a-99a5-76aabd0633d9) has error: code= message=Connection timeout to host xxx raised=aiohttp.client_exceptions.ServerTimeoutError url=xxx!\n    Airports in Romania (http://lala/dataset/ourairports-rom) with missing maintainer and organization administrators blah3disp (blah3@blah.com), blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: every year and freshness: Overdue\n        Resource List of airports in Romania (89b35e5b-32ea-4470-a854-95e47fe1a958) has error: Fail!\n    Airports in Somewhere (http://lala/dataset/ourairports-som)  Airports in Samoa (http://lala/dataset/ourairports-wsm)\n\n\nBest wishes,\nHDX Team',
                     '<html>\n  <head></head>\n  <body>\n    <span>Dear system administrator,<br><br>The following datasets have broken resources:<br><br><b>ClientConnectorError</b><br><b><i>OCHA Somalia</i></b><br>&nbsp&nbsp&nbsp&nbsp<a href="http://lala/dataset/projected-ipc-population-estimates-february-june-2016">Projected IPC population Estimates February - June 2016</a> maintained by <a href="mailto:blah@blah.com">blahdisp</a> with expected update frequency: every six months and freshness: Delinquent<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource Rural-Urban-and-IDP-Projected-Population-February-June-2016[1].xlsx (93a361c6-ace7-4cea-8407-ffd2c30d0853) has error: error: code= message=Cannot connect to host xxx ssl:False [Connection refused] raised=aiohttp.client_exceptions.ClientConnectorError url=xxx!<br><br><b>ClientConnectorSSLError</b><br><b><i>OCHA Yemen</i></b><br>&nbsp&nbsp&nbsp&nbsp<a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with missing maintainer and organization administrators <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: every year and freshness: Delinquent<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource Admin-1.zip (69146e1e-62c7-4e7f-8f6c-2dacffe02283) has error: error: code= message=Cannot connect to host xxx ssl:True [[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed (_ssl.c:645)] raised=aiohttp.client_exceptions.ClientConnectorSSLError url=xxx!<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource Admin-2.zip (f60035dc-624a-49cf-95de-9d489c07d3b9) has error: error: code= message=Cannot connect to host xxx ssl:True [[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed (_ssl.c:645)] raised=aiohttp.client_exceptions.ClientConnectorSSLError url=xxx!<br><br><b>ClientResponseError</b><br><b><i>OCHA Colombia</i></b><br>&nbsp&nbsp&nbsp&nbsp<a href="http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015">Tendencias Humanitarias y Paz - Nov 2012 - Dic 2015</a> maintained by <a href="mailto:blah@blah.com">blahdisp</a> with expected update frequency: every six months and freshness: Delinquent<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource 160304Tendencias_Humanitarias_2016_I.xlsx (256d8b17-5975-4be6-8985-5df18dda061e) has error: code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx!<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource 160304Tendencias_Humanitarias_2016_I_2.xlsx (256d8b17-5975-4be6-8985-5df18dda061a) has error: code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx!<br><br><b>Server Error (may be temporary)</b><br><b><i>OCHA Yemen</i></b><br>&nbsp&nbsp&nbsp&nbsp<a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with missing maintainer and organization administrators <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: every year and freshness: Delinquent<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource Admin-0.zip (2ade2886-2990-41d0-a89b-33c5d1de6e3a) has error: Fail!<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource Admin-3.zip (f60035dc-624a-49cf-95de-9d489c07d3ba) has error: Fail!<br><b><i>OurAirports</i></b><br>&nbsp&nbsp&nbsp&nbsp<a href="http://lala/dataset/ourairports-myt">Airports in Mayotte</a> maintained by <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: every year and freshness: Delinquent<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource List of airports in Mayotte (HXL tags) (89a99c0a-4cbf-4717-9cde-987042bc435f) has error: code= message=Connection timeout to host xxx raised=aiohttp.client_exceptions.ServerTimeoutError url=xxx!<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource List of airports in Mayotte (no HXL tags) (a5797320-ce50-4b3a-99a5-76aabd0633d9) has error: code= message=Connection timeout to host xxx raised=aiohttp.client_exceptions.ServerTimeoutError url=xxx!<br>&nbsp&nbsp&nbsp&nbsp<a href="http://lala/dataset/ourairports-rom">Airports in Romania</a> with missing maintainer and organization administrators <a href="mailto:blah3@blah.com">blah3disp</a>, <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: every year and freshness: Overdue<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource List of airports in Romania (89b35e5b-32ea-4470-a854-95e47fe1a958) has error: Fail!<br>&nbsp&nbsp&nbsp&nbsp<a href="http://lala/dataset/ourairports-som">Airports in Somewhere</a>&nbsp&nbsp<a href="http://lala/dataset/ourairports-wsm">Airports in Samoa</a><br><br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n')]
            assert TestDataFreshnessStatus.cells_result == [
                ['URL', 'Title', 'Organisation', 'Maintainer', 'Maintainer Email', 'Org Admins', 'Org Admin Emails',
                 'Update Frequency', 'Latest of Modifieds', 'Freshness', 'Error Type', 'Error', 'Date Added',
                 'No. Times',
                 'Assigned', 'Status'],
                ['http://lala/dataset/projected-ipc-population-estimates-february-june-2016',
                 'Projected IPC population Estimates February - June 2016', 'OCHA Somalia', 'blahdisp', 'blah@blah.com',
                 'blah3disp,blah4disp', 'blah3@blah.com,blah4@blah.com', 'every six months',
                 '2016-07-17T10:13:34.099517',
                 'Delinquent', 'ClientConnectorError',
                 'Rural-Urban-and-IDP-Projected-Population-February-June-2016[1].xlsx:error: code= message=Cannot connect to host xxx ssl:False [Connection refused] raised=aiohttp.client_exceptions.ClientConnectorError url=xxx',
                 '2017-02-03T19:07:30.333492', 1, 'Peter', ''],
                ['http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015',
                 'Tendencias Humanitarias y Paz - Nov 2012 - Dic 2015', 'OCHA Colombia', 'blahdisp', 'blah@blah.com',
                 'blah3disp,blah5full', 'blah3@blah.com,blah5@blah.com', 'every six months',
                 '2016-07-17T10:25:57.626518',
                 'Delinquent', 'ClientResponseError',
                 '160304Tendencias_Humanitarias_2016_I.xlsx:code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx\n160304Tendencias_Humanitarias_2016_I_2.xlsx:code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx',
                 '2017-02-03T19:07:30.333492', 1, 'Peter', ''],
                ['http://lala/dataset/ourairports-myt', 'Airports in Mayotte', 'OurAirports', 'blah5full',
                 'blah5@blah.com',
                 'blah3disp,blah4disp,blah5full', 'blah3@blah.com,blah4@blah.com,blah5@blah.com', 'every year',
                 '2015-11-24T23:32:32.025059', 'Delinquent', 'Server Error (may be temporary)',
                 'List of airports in Mayotte (HXL tags):code= message=Connection timeout to host xxx raised=aiohttp.client_exceptions.ServerTimeoutError url=xxx\nList of airports in Mayotte (no HXL tags):code= message=Connection timeout to host xxx raised=aiohttp.client_exceptions.ServerTimeoutError url=xxx',
                 '2017-02-03T19:07:30.333492', 1, 'Peter', ''],
                ['http://lala/dataset/ourairports-rom', 'Airports in Romania', 'OurAirports', '', '',
                 'blah3disp,blah4disp,blah5full', 'blah3@blah.com,blah4@blah.com,blah5@blah.com', 'every year',
                 '2015-11-24T23:36:47.280228', 'Overdue', 'Server Error (may be temporary)',
                 'List of airports in Romania:Fail', '2017-02-03T19:07:30.333492', 1, 'Peter', ''],
                ['http://lala/dataset/ourairports-som', 'Airports in Somewhere', 'OurAirports', '', '',
                 'blah3disp,blah4disp,blah5full', 'blah3@blah.com,blah4@blah.com,blah5@blah.com', 'every year',
                 '2015-11-24T23:36:47.280228', 'Overdue', 'Server Error (may be temporary)',
                 'List of airports in Somewhere:Fail', '2017-02-03T19:07:30.333492', 1, 'Peter', ''],
                ['http://lala/dataset/ourairports-wsm', 'Airports in Samoa', 'OurAirports', '', '',
                 'blah3disp,blah4disp,blah5full', 'blah3@blah.com,blah4@blah.com,blah5@blah.com', 'every year',
                 '2015-11-24T23:32:30.661408', 'Delinquent', 'Server Error (may be temporary)',
                 'List of airports in Samoa (HXL tags):Fail\nList of airports in Samoa (no HXL tags):Fail',
                 '2017-02-03T19:07:30.333492', 1, 'Peter', ''],
                ['http://lala/dataset/yemen-admin-boundaries', 'Yemen - Administrative Boundaries', 'OCHA Yemen', '',
                 '',
                 'blah4disp,blah5full', 'blah4@blah.com,blah5@blah.com', 'every year', '2015-12-28T06:39:20.134647',
                 'Delinquent', 'Server Error (may be temporary)', 'Admin-0.zip:Fail\nAdmin-3.zip:Fail',
                 '2017-01-02T19:07:30.333492', 4, 'Andrew', 'Contacted Maintainer']]

            sheet.spreadsheet = TestDataFreshnessStatus.TestSpreadsheet_Broken2
            sheet.dutyofficer = 'John'
            user0 = User(users[0])
            user1 = User(users[4])
            TestDataFreshnessStatus.email_users_result = list()
            TestDataFreshnessStatus.cells_result = None
            freshness.process_broken(sendto=[user0, user1])
            assert TestDataFreshnessStatus.email_users_result == \
                   [([{'id': 'blah', 'display_name': 'blahdisp', 'fullname': 'blahfull', 'name': 'blahname',
                       'sysadmin': False, 'email': 'blah@blah.com'},
                      {'id': 'blah5', 'fullname': 'blah5full', 'name': 'blah5name',
                       'sysadmin': False, 'email': 'blah5@blah.com'}], 'Broken datasets',
                     'Dear system administrator,\n\nThe following datasets have broken resources:\n\nClientConnectorError\nOCHA Somalia\n    Projected IPC population Estimates February - June 2016 (http://lala/dataset/projected-ipc-population-estimates-february-june-2016) maintained by blahdisp (blah@blah.com) with expected update frequency: every six months and freshness: Delinquent\n        Resource Rural-Urban-and-IDP-Projected-Population-February-June-2016[1].xlsx (93a361c6-ace7-4cea-8407-ffd2c30d0853) has error: error: code= message=Cannot connect to host xxx ssl:False [Connection refused] raised=aiohttp.client_exceptions.ClientConnectorError url=xxx!\n\nClientConnectorSSLError\nOCHA Yemen\n    Yemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with missing maintainer and organization administrators blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: every year and freshness: Delinquent\n        Resource Admin-1.zip (69146e1e-62c7-4e7f-8f6c-2dacffe02283) has error: error: code= message=Cannot connect to host xxx ssl:True [[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed (_ssl.c:645)] raised=aiohttp.client_exceptions.ClientConnectorSSLError url=xxx!\n        Resource Admin-2.zip (f60035dc-624a-49cf-95de-9d489c07d3b9) has error: error: code= message=Cannot connect to host xxx ssl:True [[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed (_ssl.c:645)] raised=aiohttp.client_exceptions.ClientConnectorSSLError url=xxx!\n\nClientResponseError\nOCHA Colombia\n    Tendencias Humanitarias y Paz - Nov 2012 - Dic 2015 (http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015) maintained by blahdisp (blah@blah.com) with expected update frequency: every six months and freshness: Delinquent\n        Resource 160304Tendencias_Humanitarias_2016_I.xlsx (256d8b17-5975-4be6-8985-5df18dda061e) has error: code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx!\n        Resource 160304Tendencias_Humanitarias_2016_I_2.xlsx (256d8b17-5975-4be6-8985-5df18dda061a) has error: code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx!\n\nServer Error (may be temporary)\nOCHA Yemen\n    Yemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with missing maintainer and organization administrators blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: every year and freshness: Delinquent\n        Resource Admin-0.zip (2ade2886-2990-41d0-a89b-33c5d1de6e3a) has error: Fail!\n        Resource Admin-3.zip (f60035dc-624a-49cf-95de-9d489c07d3ba) has error: Fail!\nOurAirports\n    Airports in Mayotte (http://lala/dataset/ourairports-myt) maintained by blah5full (blah5@blah.com) with expected update frequency: every year and freshness: Delinquent\n        Resource List of airports in Mayotte (HXL tags) (89a99c0a-4cbf-4717-9cde-987042bc435f) has error: code= message=Connection timeout to host xxx raised=aiohttp.client_exceptions.ServerTimeoutError url=xxx!\n        Resource List of airports in Mayotte (no HXL tags) (a5797320-ce50-4b3a-99a5-76aabd0633d9) has error: code= message=Connection timeout to host xxx raised=aiohttp.client_exceptions.ServerTimeoutError url=xxx!\n    Airports in Romania (http://lala/dataset/ourairports-rom) with missing maintainer and organization administrators blah3disp (blah3@blah.com), blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: every year and freshness: Overdue\n        Resource List of airports in Romania (89b35e5b-32ea-4470-a854-95e47fe1a958) has error: Fail!\n    Airports in Somewhere (http://lala/dataset/ourairports-som)  Airports in Samoa (http://lala/dataset/ourairports-wsm)\n\n\nBest wishes,\nHDX Team',
                     '<html>\n  <head></head>\n  <body>\n    <span>Dear system administrator,<br><br>The following datasets have broken resources:<br><br><b>ClientConnectorError</b><br><b><i>OCHA Somalia</i></b><br>&nbsp&nbsp&nbsp&nbsp<a href="http://lala/dataset/projected-ipc-population-estimates-february-june-2016">Projected IPC population Estimates February - June 2016</a> maintained by <a href="mailto:blah@blah.com">blahdisp</a> with expected update frequency: every six months and freshness: Delinquent<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource Rural-Urban-and-IDP-Projected-Population-February-June-2016[1].xlsx (93a361c6-ace7-4cea-8407-ffd2c30d0853) has error: error: code= message=Cannot connect to host xxx ssl:False [Connection refused] raised=aiohttp.client_exceptions.ClientConnectorError url=xxx!<br><br><b>ClientConnectorSSLError</b><br><b><i>OCHA Yemen</i></b><br>&nbsp&nbsp&nbsp&nbsp<a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with missing maintainer and organization administrators <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: every year and freshness: Delinquent<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource Admin-1.zip (69146e1e-62c7-4e7f-8f6c-2dacffe02283) has error: error: code= message=Cannot connect to host xxx ssl:True [[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed (_ssl.c:645)] raised=aiohttp.client_exceptions.ClientConnectorSSLError url=xxx!<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource Admin-2.zip (f60035dc-624a-49cf-95de-9d489c07d3b9) has error: error: code= message=Cannot connect to host xxx ssl:True [[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed (_ssl.c:645)] raised=aiohttp.client_exceptions.ClientConnectorSSLError url=xxx!<br><br><b>ClientResponseError</b><br><b><i>OCHA Colombia</i></b><br>&nbsp&nbsp&nbsp&nbsp<a href="http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015">Tendencias Humanitarias y Paz - Nov 2012 - Dic 2015</a> maintained by <a href="mailto:blah@blah.com">blahdisp</a> with expected update frequency: every six months and freshness: Delinquent<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource 160304Tendencias_Humanitarias_2016_I.xlsx (256d8b17-5975-4be6-8985-5df18dda061e) has error: code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx!<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource 160304Tendencias_Humanitarias_2016_I_2.xlsx (256d8b17-5975-4be6-8985-5df18dda061a) has error: code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx!<br><br><b>Server Error (may be temporary)</b><br><b><i>OCHA Yemen</i></b><br>&nbsp&nbsp&nbsp&nbsp<a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with missing maintainer and organization administrators <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: every year and freshness: Delinquent<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource Admin-0.zip (2ade2886-2990-41d0-a89b-33c5d1de6e3a) has error: Fail!<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource Admin-3.zip (f60035dc-624a-49cf-95de-9d489c07d3ba) has error: Fail!<br><b><i>OurAirports</i></b><br>&nbsp&nbsp&nbsp&nbsp<a href="http://lala/dataset/ourairports-myt">Airports in Mayotte</a> maintained by <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: every year and freshness: Delinquent<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource List of airports in Mayotte (HXL tags) (89a99c0a-4cbf-4717-9cde-987042bc435f) has error: code= message=Connection timeout to host xxx raised=aiohttp.client_exceptions.ServerTimeoutError url=xxx!<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource List of airports in Mayotte (no HXL tags) (a5797320-ce50-4b3a-99a5-76aabd0633d9) has error: code= message=Connection timeout to host xxx raised=aiohttp.client_exceptions.ServerTimeoutError url=xxx!<br>&nbsp&nbsp&nbsp&nbsp<a href="http://lala/dataset/ourairports-rom">Airports in Romania</a> with missing maintainer and organization administrators <a href="mailto:blah3@blah.com">blah3disp</a>, <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: every year and freshness: Overdue<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource List of airports in Romania (89b35e5b-32ea-4470-a854-95e47fe1a958) has error: Fail!<br>&nbsp&nbsp&nbsp&nbsp<a href="http://lala/dataset/ourairports-som">Airports in Somewhere</a>&nbsp&nbsp<a href="http://lala/dataset/ourairports-wsm">Airports in Samoa</a><br><br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n')]
            assert TestDataFreshnessStatus.cells_result == [
                ['URL', 'Title', 'Organisation', 'Maintainer', 'Maintainer Email', 'Org Admins', 'Org Admin Emails',
                 'Update Frequency', 'Latest of Modifieds', 'Freshness', 'Error Type', 'Error', 'Date Added',
                 'No. Times',
                 'Assigned', 'Status'],
                ['http://lala/dataset/projected-ipc-population-estimates-february-june-2016',
                 'Projected IPC population Estimates February - June 2016', 'OCHA Somalia', 'blahdisp', 'blah@blah.com',
                 'blah3disp,blah4disp', 'blah3@blah.com,blah4@blah.com', 'every six months',
                 '2016-07-17T10:13:34.099517',
                 'Delinquent', 'ClientConnectorError',
                 'Rural-Urban-and-IDP-Projected-Population-February-June-2016[1].xlsx:error: code= message=Cannot connect to host xxx ssl:False [Connection refused] raised=aiohttp.client_exceptions.ClientConnectorError url=xxx',
                 '2017-02-03T19:07:30.333492', 1, 'John', ''],
                ['http://lala/dataset/yemen-admin-boundaries', 'Yemen - Administrative Boundaries', 'OCHA Yemen', '',
                 '',
                 'blah4disp,blah5full', 'blah4@blah.com,blah5@blah.com', 'every year', '2015-12-28T06:39:20.134647',
                 'Delinquent', 'Server Error (may be temporary)', 'Admin-0.zip:Fail\nAdmin-3.zip:Fail',
                 '2017-02-03T19:07:30.333492', 1, 'John', ''],
                ['http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015',
                 'Tendencias Humanitarias y Paz - Nov 2012 - Dic 2015', 'OCHA Colombia', 'blahdisp', 'blah@blah.com',
                 'blah3disp,blah5full', 'blah3@blah.com,blah5@blah.com', 'every six months',
                 '2016-07-17T10:25:57.626518',
                 'Delinquent', 'ClientResponseError',
                 '160304Tendencias_Humanitarias_2016_I.xlsx:code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx\n160304Tendencias_Humanitarias_2016_I_2.xlsx:code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx',
                 '2017-02-03T19:07:30.333492', 1, 'John', ''],
                ['http://lala/dataset/ourairports-myt', 'Airports in Mayotte', 'OurAirports', 'blah5full',
                 'blah5@blah.com',
                 'blah3disp,blah4disp,blah5full', 'blah3@blah.com,blah4@blah.com,blah5@blah.com', 'every year',
                 '2015-11-24T23:32:32.025059', 'Delinquent', 'Server Error (may be temporary)',
                 'List of airports in Mayotte (HXL tags):code= message=Connection timeout to host xxx raised=aiohttp.client_exceptions.ServerTimeoutError url=xxx\nList of airports in Mayotte (no HXL tags):code= message=Connection timeout to host xxx raised=aiohttp.client_exceptions.ServerTimeoutError url=xxx',
                 '2017-02-03T19:07:30.333492', 1, 'John', ''],
                ['http://lala/dataset/ourairports-rom', 'Airports in Romania', 'OurAirports', '', '',
                 'blah3disp,blah4disp,blah5full', 'blah3@blah.com,blah4@blah.com,blah5@blah.com', 'every year',
                 '2015-11-24T23:36:47.280228', 'Overdue', 'Server Error (may be temporary)',
                 'List of airports in Romania:Fail', '2017-02-03T19:07:30.333492', 1, 'John', ''],
                ['http://lala/dataset/ourairports-som', 'Airports in Somewhere', 'OurAirports', '', '',
                 'blah3disp,blah4disp,blah5full', 'blah3@blah.com,blah4@blah.com,blah5@blah.com', 'every year',
                 '2015-11-24T23:36:47.280228', 'Overdue', 'Server Error (may be temporary)',
                 'List of airports in Somewhere:Fail', '2017-02-03T19:07:30.333492', 1, 'John', ''],
                ['http://lala/dataset/ourairports-wsm', 'Airports in Samoa', 'OurAirports', '', '',
                 'blah3disp,blah4disp,blah5full', 'blah3@blah.com,blah4@blah.com,blah5@blah.com', 'every year',
                 '2015-11-24T23:32:30.661408', 'Delinquent', 'Server Error (may be temporary)',
                 'List of airports in Samoa (HXL tags):Fail\nList of airports in Samoa (no HXL tags):Fail',
                 '2017-02-03T19:07:30.333492', 1, 'John', '']]

            now = parser.parse('2017-01-31 19:07:30.333492')
            TestDataFreshnessStatus.email_users_result = list()
            TestDataFreshnessStatus.cells_result = None
            databasequeries = DatabaseQueries(session=session, now=now)
            freshness = DataFreshnessStatus(datasethelper=datasethelper, databasequeries=databasequeries, email=email,
                                            sheet=sheet)

            sheet.spreadsheet = TestDataFreshnessStatus.TestSpreadsheet_Broken1
            sheet.dutyofficer = 'Peter'
            TestDataFreshnessStatus.email_users_result = list()
            TestDataFreshnessStatus.cells_result = None
            freshness.process_broken()
            assert TestDataFreshnessStatus.email_users_result == list()
            assert TestDataFreshnessStatus.cells_result is None

    def test_freshnessstatus(self, configuration, database_status, users, organizations):
        site_url = 'http://lala/'
        sysadmins_to_email = ['blah2@blah.com', 'blah4@blah.com']
        now = parser.parse('2017-02-02 19:07:30.333492')
        sheet = Sheet(now)
        email = Email(send_emails=self.email_users)
        with Database(**database_status) as session:
            datasethelper = DatasetHelper(site_url=site_url, users=users, organizations=organizations,
                                          sysadmins_to_email=sysadmins_to_email)
            databasequeries = DatabaseQueries(session=session, now=now)
            freshness = DataFreshnessStatus(datasethelper=datasethelper, databasequeries=databasequeries, email=email,
                                            sheet=sheet)
            sheet.spreadsheet = TestDataFreshnessStatus.TestSpreadsheet_OverdueDelinquent
            sheet.dutyofficer = 'Sharon'

            TestDataFreshnessStatus.email_users_result = list()
            freshness.check_number_datasets(now)
            assert TestDataFreshnessStatus.email_users_result == [(['blah2@blah.com', 'blah4@blah.com'],
                                                                   'WARNING: Fall in datasets on HDX today!',
                                                                   'Dear system administrator,\n\nThere are 1 (16%) fewer datasets today than yesterday on HDX!\n\nBest wishes,\nHDX Team',
                                                                   '<html>\n  <head></head>\n  <body>\n    <span>Dear system administrator,<br><br>There are 1 (16%) fewer datasets today than yesterday on HDX!<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n')]

            TestDataFreshnessStatus.email_users_result = list()
            TestDataFreshnessStatus.cells_result = None
            freshness.process_delinquent()
            assert TestDataFreshnessStatus.email_users_result == \
                   [(['blah2@blah.com', 'blah4@blah.com'], 'Delinquent datasets',
                     'Dear system administrator,\n\nThe following datasets have just become delinquent:\n\nAirports in Mayotte (http://lala/dataset/ourairports-myt) from OurAirports maintained by blah5full (blah5@blah.com) with expected update frequency: every year\nAirports in Samoa (http://lala/dataset/ourairports-wsm) from OurAirports with missing maintainer and organization administrators blah3disp (blah3@blah.com), blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: every year\n\nBest wishes,\nHDX Team',
                     '<html>\n  <head></head>\n  <body>\n    <span>Dear system administrator,<br><br>The following datasets have just become delinquent:<br><br><a href="http://lala/dataset/ourairports-myt">Airports in Mayotte</a> from OurAirports maintained by <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: every year<br><a href="http://lala/dataset/ourairports-wsm">Airports in Samoa</a> from OurAirports with missing maintainer and organization administrators <a href="mailto:blah3@blah.com">blah3disp</a>, <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: every year<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n')]
            assert TestDataFreshnessStatus.cells_result == [
                ['URL', 'Title', 'Organisation', 'Maintainer', 'Maintainer Email', 'Org Admins', 'Org Admin Emails',
                 'Update Frequency', 'Latest of Modifieds', 'Date Added', 'No. Times', 'Assigned', 'Status'],
                ['http://lala/dataset/ourairports-wsm', 'Airports in Samoa', 'OurAirports', '', '',
                 'blah3disp,blah4disp,blah5full', 'blah3@blah.com,blah4@blah.com,blah5@blah.com', 'every year',
                 '2015-11-24T23:32:30.661408', '2017-02-02T19:07:30.333492', 1, 'Sharon', ''],
                ['http://lala/dataset/ourairports-myt', 'Airports in Mayotte', 'OurAirports', 'blah5full',
                 'blah5@blah.com',
                 'blah3disp,blah4disp,blah5full', 'blah3@blah.com,blah4@blah.com,blah5@blah.com', 'every year',
                 '2015-11-24T23:32:32.025059', '2017-01-01T19:07:30.333492', 3, 'Peter', 'Done']]

            expected_result = \
                [([{'sysadmin': False, 'email': 'blah@blah.com', 'id': 'blah', 'name': 'blahname',
                    'display_name': 'blahdisp', 'fullname': 'blahfull'}], 'Time to update your datasets on HDX',
                  'Dear blahdisp,\n\nThe dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your dashboard on HDX.\n\nTendencias Humanitarias y Paz - Nov 2012 - Dic 2015 (http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015) with expected update frequency: every six months\nProjected IPC population Estimates February - June 2016 (http://lala/dataset/projected-ipc-population-estimates-february-june-2016) with expected update frequency: every six months\n\nTip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.\n\nBest wishes,\nHDX Team',
                  '<html>\n  <head></head>\n  <body>\n    <span>Dear blahdisp,<br><br>The dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your <a href="https://data.humdata.org/dashboard/datasets">dashboard</a> on HDX.<br><br><a href="http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015">Tendencias Humanitarias y Paz - Nov 2012 - Dic 2015</a> with expected update frequency: every six months<br><a href="http://lala/dataset/projected-ipc-population-estimates-february-june-2016">Projected IPC population Estimates February - June 2016</a> with expected update frequency: every six months<br><br>Tip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n'),
                 ([{'sysadmin': True, 'email': 'blah4@blah.com', 'id': 'blah4', 'name': 'blah4name',
                    'display_name': 'blah4disp', 'fullname': 'blah4full'}], 'Time to update your datasets on HDX',
                  'Dear blah4disp,\n\nThe dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your dashboard on HDX.\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with expected update frequency: every year\n\nTip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.\n\nBest wishes,\nHDX Team',
                  '<html>\n  <head></head>\n  <body>\n    <span>Dear blah4disp,<br><br>The dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your <a href="https://data.humdata.org/dashboard/datasets">dashboard</a> on HDX.<br><br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with expected update frequency: every year<br><br>Tip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n'),
                 ([{'name': 'blah5name', 'sysadmin': False, 'email': 'blah5@blah.com', 'fullname': 'blah5full',
                    'id': 'blah5'}], 'Time to update your datasets on HDX',
                  'Dear blah5full,\n\nThe dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your dashboard on HDX.\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with expected update frequency: every year\n\nTip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.\n\nBest wishes,\nHDX Team',
                  '<html>\n  <head></head>\n  <body>\n    <span>Dear blah5full,<br><br>The dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your <a href="https://data.humdata.org/dashboard/datasets">dashboard</a> on HDX.<br><br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with expected update frequency: every year<br><br>Tip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n')]
            TestDataFreshnessStatus.email_users_result = list()
            freshness.process_overdue()
            assert TestDataFreshnessStatus.email_users_result == expected_result
            TestDataFreshnessStatus.email_users_result = list()
            freshness.process_overdue(sysadmins=[User(users[0])])
            expected_result.append(
                ([{'email': 'blah@blah.com', 'id': 'blah', 'name': 'blahname', 'sysadmin': False,
                   'fullname': 'blahfull', 'display_name': 'blahdisp'}], 'All overdue dataset emails',
                 'Dear system administrator,\n\nDear blahdisp,\n\nThe dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your dashboard on HDX.\n\nTendencias Humanitarias y Paz - Nov 2012 - Dic 2015 (http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015) with expected update frequency: every six months\nProjected IPC population Estimates February - June 2016 (http://lala/dataset/projected-ipc-population-estimates-february-june-2016) with expected update frequency: every six months\nDear blah4disp,\n\nThe dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your dashboard on HDX.\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with expected update frequency: every year\nDear blah5full,\n\nThe dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your dashboard on HDX.\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with expected update frequency: every year\n\nBest wishes,\nHDX Team',
                 '<html>\n  <head></head>\n  <body>\n    <span>Dear system administrator,<br><br>Dear blahdisp,<br><br>The dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your dashboard on HDX.<br><br><a href="http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015">Tendencias Humanitarias y Paz - Nov 2012 - Dic 2015</a> with expected update frequency: every six months<br><a href="http://lala/dataset/projected-ipc-population-estimates-february-june-2016">Projected IPC population Estimates February - June 2016</a> with expected update frequency: every six months<br>Dear blah4disp,<br><br>The dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your dashboard on HDX.<br><br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with expected update frequency: every year<br>Dear blah5full,<br><br>The dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your dashboard on HDX.<br><br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with expected update frequency: every year<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n'))
            assert TestDataFreshnessStatus.email_users_result == expected_result
            TestDataFreshnessStatus.email_users_result = list()
            user0 = User(users[0])
            user1 = User(users[1])
            freshness.process_overdue(sendto=[user0, user1])
            assert TestDataFreshnessStatus.email_users_result == \
                   [([{'fullname': 'blahfull', 'sysadmin': False, 'email': 'blah@blah.com', 'id': 'blah',
                       'display_name': 'blahdisp', 'name': 'blahname'},
                      {'fullname': 'blah2full', 'name': 'blah2name', 'email': 'blah2@blah.com', 'sysadmin': True,
                       'id': 'blah2'}], 'Time to update your datasets on HDX',
                     'Dear blahdisp,\n\nThe dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your dashboard on HDX.\n\nTendencias Humanitarias y Paz - Nov 2012 - Dic 2015 (http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015) with expected update frequency: every six months\nProjected IPC population Estimates February - June 2016 (http://lala/dataset/projected-ipc-population-estimates-february-june-2016) with expected update frequency: every six months\n\nTip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.\n\nBest wishes,\nHDX Team',
                     '<html>\n  <head></head>\n  <body>\n    <span>Dear blahdisp,<br><br>The dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your <a href="https://data.humdata.org/dashboard/datasets">dashboard</a> on HDX.<br><br><a href="http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015">Tendencias Humanitarias y Paz - Nov 2012 - Dic 2015</a> with expected update frequency: every six months<br><a href="http://lala/dataset/projected-ipc-population-estimates-february-june-2016">Projected IPC population Estimates February - June 2016</a> with expected update frequency: every six months<br><br>Tip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n'),
                    ([{'fullname': 'blahfull', 'sysadmin': False, 'email': 'blah@blah.com', 'id': 'blah',
                       'display_name': 'blahdisp', 'name': 'blahname'},
                      {'fullname': 'blah2full', 'name': 'blah2name', 'email': 'blah2@blah.com', 'sysadmin': True,
                       'id': 'blah2'}], 'Time to update your datasets on HDX',
                     'Dear blah4disp,\n\nThe dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your dashboard on HDX.\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with expected update frequency: every year\n\nTip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.\n\nBest wishes,\nHDX Team',
                     '<html>\n  <head></head>\n  <body>\n    <span>Dear blah4disp,<br><br>The dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your <a href="https://data.humdata.org/dashboard/datasets">dashboard</a> on HDX.<br><br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with expected update frequency: every year<br><br>Tip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n'),
                    ([{'fullname': 'blahfull', 'sysadmin': False, 'email': 'blah@blah.com', 'id': 'blah',
                       'display_name': 'blahdisp', 'name': 'blahname'},
                      {'fullname': 'blah2full', 'name': 'blah2name', 'email': 'blah2@blah.com', 'sysadmin': True,
                       'id': 'blah2'}], 'Time to update your datasets on HDX',
                     'Dear blah5full,\n\nThe dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your dashboard on HDX.\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with expected update frequency: every year\n\nTip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.\n\nBest wishes,\nHDX Team',
                     '<html>\n  <head></head>\n  <body>\n    <span>Dear blah5full,<br><br>The dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your <a href="https://data.humdata.org/dashboard/datasets">dashboard</a> on HDX.<br><br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with expected update frequency: every year<br><br>Tip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n')]

            now = parser.parse('2017-01-31 19:07:30.333492')
            databasequeries = DatabaseQueries(session=session, now=now)
            freshness = DataFreshnessStatus(datasethelper=datasethelper, databasequeries=databasequeries, email=email,
                                            sheet=sheet)
            sheet.spreadsheet = TestDataFreshnessStatus.TestSpreadsheet_OverdueDelinquent
            sheet.dutyofficer = 'Sharon'

            TestDataFreshnessStatus.email_users_result = list()
            TestDataFreshnessStatus.cells_result = None
            freshness.process_delinquent()
            assert TestDataFreshnessStatus.email_users_result == list()
            assert TestDataFreshnessStatus.cells_result is None
            TestDataFreshnessStatus.email_users_result = list()
            freshness.process_overdue()
            assert TestDataFreshnessStatus.email_users_result == list()

    def test_freshnessmaintainerorgadmins(self, configuration, database_maintainer, users, organizations):
        site_url = 'http://lala/'
        sysadmins_to_email = ['blah2@blah.com', 'blah4@blah.com']
        now = parser.parse('2017-02-02 19:07:30.333492')
        sheet = Sheet(now)
        email = Email(send_emails=self.email_users)
        with Database(**database_maintainer) as session:
            datasethelper = DatasetHelper(site_url=site_url, users=users, organizations=organizations,
                                          sysadmins_to_email=sysadmins_to_email)
            databasequeries = DatabaseQueries(session=session, now=now)
            freshness = DataFreshnessStatus(datasethelper=datasethelper, databasequeries=databasequeries, email=email,
                                            sheet=sheet)
            sheet.spreadsheet = TestDataFreshnessStatus.TestSpreadsheet_MaintainerOrgAdmins
            sheet.dutyofficer = 'Aaron'

            TestDataFreshnessStatus.email_users_result = list()
            TestDataFreshnessStatus.cells_result = list()
            freshness.process_maintainer_orgadmins()
            assert TestDataFreshnessStatus.email_users_result == \
                   [(['blah2@blah.com', 'blah4@blah.com'], 'Datasets with invalid maintainer',
                     'Dear system administrator,\n\nThe following datasets have an invalid maintainer:\n\nTendencias Humanitarias y Paz - Nov 2012 - Dic 2015 (http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015) from OCHA Colombia maintained by blahdisp (blah@blah.com) with expected update frequency: every six months\nProjected IPC population Estimates February - June 2016 (http://lala/dataset/projected-ipc-population-estimates-february-june-2016) from OCHA Somalia maintained by blahdisp (blah@blah.com) with expected update frequency: every six months\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) from OCHA Yemen with missing maintainer and organization administrators blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: every year\nAirports in Samoa (http://lala/dataset/ourairports-wsm) from OurAirports with missing maintainer and organization administrators blah3disp (blah3@blah.com), blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: every year\n\nBest wishes,\nHDX Team',
                     '<html>\n  <head></head>\n  <body>\n    <span>Dear system administrator,<br><br>The following datasets have an invalid maintainer:<br><br><a href="http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015">Tendencias Humanitarias y Paz - Nov 2012 - Dic 2015</a> from OCHA Colombia maintained by <a href="mailto:blah@blah.com">blahdisp</a> with expected update frequency: every six months<br><a href="http://lala/dataset/projected-ipc-population-estimates-february-june-2016">Projected IPC population Estimates February - June 2016</a> from OCHA Somalia maintained by <a href="mailto:blah@blah.com">blahdisp</a> with expected update frequency: every six months<br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> from OCHA Yemen with missing maintainer and organization administrators <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: every year<br><a href="http://lala/dataset/ourairports-wsm">Airports in Samoa</a> from OurAirports with missing maintainer and organization administrators <a href="mailto:blah3@blah.com">blah3disp</a>, <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: every year<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n'),
                    (['blah2@blah.com', 'blah4@blah.com'], 'Organizations with invalid admins',
                     'Dear system administrator,\n\nThe following organizations have an invalid admin:\n\nOCHA Somalia (http://lala/organization/ocha-somalia) with error: All org admins are sysadmins!\n\nBest wishes,\nHDX Team',
                     '<html>\n  <head></head>\n  <body>\n    <span>Dear system administrator,<br><br>The following organizations have an invalid admin:<br><br><a href="http://lala/organization/ocha-somalia">OCHA Somalia</a> with error: All org admins are sysadmins!<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n')]

            assert TestDataFreshnessStatus.cells_result == [[
                ['URL', 'Title', 'Organisation', 'Maintainer', 'Maintainer Email', 'Org Admins', 'Org Admin Emails',
                 'Update Frequency', 'Latest of Modifieds', 'Date Added', 'No. Times', 'Assigned', 'Status'],
                ['http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015',
                 'Tendencias Humanitarias y Paz - Nov 2012 - Dic 2015', 'OCHA Colombia', 'blahdisp', 'blah@blah.com',
                 'blah3disp,blah5full', 'blah3@blah.com,blah5@blah.com', 'every six months',
                 '2016-07-17T10:25:57.626518',
                 '2017-02-02T19:07:30.333492', 1, 'Aaron', ''],
                ['http://lala/dataset/projected-ipc-population-estimates-february-june-2016',
                 'Projected IPC population Estimates February - June 2016', 'OCHA Somalia', 'blahdisp', 'blah@blah.com',
                 'blah3disp,blah4disp', 'blah3@blah.com,blah4@blah.com', 'every six months',
                 '2016-07-17T10:13:34.099517',
                 '2017-02-02T19:07:30.333492', 1, 'Aaron', ''],
                ['http://lala/dataset/yemen-admin-boundaries', 'Yemen - Administrative Boundaries', 'OCHA Yemen', '',
                 '',
                 'blah4disp,blah5full', 'blah4@blah.com,blah5@blah.com', 'every year', '2015-12-28T06:39:20.134647',
                 '2017-02-02T19:07:30.333492', 1, 'Aaron', ''],
                ['http://lala/dataset/ourairports-wsm', 'Airports in Samoa', 'OurAirports', '', '',
                 'blah3disp,blah4disp,blah5full', 'blah3@blah.com,blah4@blah.com,blah5@blah.com', 'every year',
                 '2015-11-24T23:32:30.661408', '2017-02-02T19:07:30.333492', 1, 'Aaron', ''],
                ['http://lala/dataset/ourairports-myt', 'Airports in Mayotte', 'OurAirports', 'blah5full',
                 'blah5@blah.com', 'blah3disp,blah4disp,blah5full', 'blah3@blah.com,blah4@blah.com,blah5@blah.com',
                 'every year', '2015-11-24T23:32:32.025059', '2017-01-01T19:07:30.333492', 3, 'Aaron', 'Done']],
                [['URL', 'Title', 'Error', 'Date Added', 'No. Times', 'Assigned', 'Status'],
                 ['http://lala/organization/ocha-somalia', 'OCHA Somalia', 'All org admins are sysadmins!',
                  '2017-01-01T19:07:30.333492', 6, 'Aaron', '']]]

            neworgs = copy.deepcopy(organizations)
            neworgs[0]['users'].append(
                {'capacity': 'editor', 'id': 'blah', 'name': 'blahname', 'sysadmin': False, 'fullname': 'blahfull',
                 'display_name': 'blahdisp'})
            neworgs[1]['users'] = list()
            datasethelper = DatasetHelper(site_url=site_url, users=users, organizations=neworgs,
                                          sysadmins_to_email=sysadmins_to_email)
            databasequeries = DatabaseQueries(session=session, now=now)
            freshness = DataFreshnessStatus(datasethelper=datasethelper, databasequeries=databasequeries, email=email,
                                            sheet=sheet)
            TestDataFreshnessStatus.email_users_result = list()
            freshness.process_maintainer_orgadmins()
            assert TestDataFreshnessStatus.email_users_result == \
                   [(['blah2@blah.com', 'blah4@blah.com'], 'Datasets with invalid maintainer',
                     'Dear system administrator,\n\nThe following datasets have an invalid maintainer:\n\nProjected IPC population Estimates February - June 2016 (http://lala/dataset/projected-ipc-population-estimates-february-june-2016) from OCHA Somalia maintained by blahdisp (blah@blah.com) with expected update frequency: every six months\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) from OCHA Yemen with missing maintainer and organization administrators blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: every year\nAirports in Samoa (http://lala/dataset/ourairports-wsm) from OurAirports with missing maintainer and organization administrators blah3disp (blah3@blah.com), blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: every year\n\nBest wishes,\nHDX Team',
                     '<html>\n  <head></head>\n  <body>\n    <span>Dear system administrator,<br><br>The following datasets have an invalid maintainer:<br><br><a href="http://lala/dataset/projected-ipc-population-estimates-february-june-2016">Projected IPC population Estimates February - June 2016</a> from OCHA Somalia maintained by <a href="mailto:blah@blah.com">blahdisp</a> with expected update frequency: every six months<br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> from OCHA Yemen with missing maintainer and organization administrators <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: every year<br><a href="http://lala/dataset/ourairports-wsm">Airports in Samoa</a> from OurAirports with missing maintainer and organization administrators <a href="mailto:blah3@blah.com">blah3disp</a>, <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: every year<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n'),
                    (['blah2@blah.com', 'blah4@blah.com'], 'Organizations with invalid admins',
                     'Dear system administrator,\n\nThe following organizations have an invalid admin:\n\nOCHA Somalia (http://lala/organization/ocha-somalia) with error: No org admins defined!\n\nBest wishes,\nHDX Team',
                     '<html>\n  <head></head>\n  <body>\n    <span>Dear system administrator,<br><br>The following organizations have an invalid admin:<br><br><a href="http://lala/organization/ocha-somalia">OCHA Somalia</a> with error: No org admins defined!<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n')]
            neworgs = copy.deepcopy(organizations)
            neworgs[0]['users'].append(
                {'capacity': 'member', 'id': 'blah', 'name': 'blahname', 'sysadmin': False, 'fullname': 'blahfull',
                 'display_name': 'blahdisp'})
            neworgs[1]['users'][0]['id'] = 'NOTEXIST1'
            neworgs[1]['users'][1]['id'] = 'NOTEXIST2'
            datasethelper = DatasetHelper(site_url=site_url, users=users, organizations=neworgs,
                                          sysadmins_to_email=sysadmins_to_email)
            databasequeries = DatabaseQueries(session=session, now=now)
            freshness = DataFreshnessStatus(datasethelper=datasethelper, databasequeries=databasequeries, email=email,
                                            sheet=sheet)
            TestDataFreshnessStatus.email_users_result = list()
            freshness.process_maintainer_orgadmins()
            assert TestDataFreshnessStatus.email_users_result == \
                   [(['blah2@blah.com', 'blah4@blah.com'], 'Datasets with invalid maintainer',
                     'Dear system administrator,\n\nThe following datasets have an invalid maintainer:\n\nTendencias Humanitarias y Paz - Nov 2012 - Dic 2015 (http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015) from OCHA Colombia maintained by blahdisp (blah@blah.com) with expected update frequency: every six months\nProjected IPC population Estimates February - June 2016 (http://lala/dataset/projected-ipc-population-estimates-february-june-2016) from OCHA Somalia maintained by blahdisp (blah@blah.com) with expected update frequency: every six months\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) from OCHA Yemen with missing maintainer and organization administrators blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: every year\nAirports in Samoa (http://lala/dataset/ourairports-wsm) from OurAirports with missing maintainer and organization administrators blah3disp (blah3@blah.com), blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: every year\n\nBest wishes,\nHDX Team',
                     '<html>\n  <head></head>\n  <body>\n    <span>Dear system administrator,<br><br>The following datasets have an invalid maintainer:<br><br><a href="http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015">Tendencias Humanitarias y Paz - Nov 2012 - Dic 2015</a> from OCHA Colombia maintained by <a href="mailto:blah@blah.com">blahdisp</a> with expected update frequency: every six months<br><a href="http://lala/dataset/projected-ipc-population-estimates-february-june-2016">Projected IPC population Estimates February - June 2016</a> from OCHA Somalia maintained by <a href="mailto:blah@blah.com">blahdisp</a> with expected update frequency: every six months<br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> from OCHA Yemen with missing maintainer and organization administrators <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: every year<br><a href="http://lala/dataset/ourairports-wsm">Airports in Samoa</a> from OurAirports with missing maintainer and organization administrators <a href="mailto:blah3@blah.com">blah3disp</a>, <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: every year<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n'),
                    (['blah2@blah.com', 'blah4@blah.com'], 'Organizations with invalid admins',
                     'Dear system administrator,\n\nThe following organizations have an invalid admin:\n\nOCHA Somalia (http://lala/organization/ocha-somalia) with error: The following org admins do not exist: NOTEXIST1, NOTEXIST2!\n\nBest wishes,\nHDX Team',
                     '<html>\n  <head></head>\n  <body>\n    <span>Dear system administrator,<br><br>The following organizations have an invalid admin:<br><br><a href="http://lala/organization/ocha-somalia">OCHA Somalia</a> with error: The following org admins do not exist: NOTEXIST1, NOTEXIST2!<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n')]

    def test_freshnessfailure(self, configuration, database_failure, users, organizations):
        site_url = None
        TestDataFreshnessStatus.email_users_result = list()
        now = parser.parse('2017-02-03 19:07:30.333492')
        sheet = Sheet(now)
        email = Email(send_emails=self.email_users)
        with Database(**database_failure) as session:
            datasethelper = DatasetHelper(site_url=site_url, users=users, organizations=organizations,
                                          sysadmins_to_email=['blah2@blah.com', 'blah4@blah.com'])
            databasequeries = DatabaseQueries(session=session, now=now)
            freshness = DataFreshnessStatus(datasethelper=datasethelper, databasequeries=databasequeries, email=email,
                                            sheet=sheet)
            freshness.check_number_datasets(now, send_failures=['blah2@blah.com', 'blah4@blah.com'])
            assert TestDataFreshnessStatus.email_users_result == [(['blah2@blah.com', 'blah4@blah.com'],
                                                                   'FAILURE: No datasets today!',
                                                                   'Dear system administrator,\n\nIt is highly probable that data freshness has failed!\n\nBest wishes,\nHDX Team',
                                                                   '<html>\n  <head></head>\n  <body>\n    <span>Dear system administrator,<br><br>It is highly probable that data freshness has failed!<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n')]
            TestDataFreshnessStatus.email_users_result = list()
            now = parser.parse('2017-02-02 19:07:30.333492')
            databasequeries = DatabaseQueries(session=session, now=now)
            freshness = DataFreshnessStatus(datasethelper=datasethelper, databasequeries=databasequeries, email=email,
                                            sheet=sheet)
            freshness.check_number_datasets(parser.parse('2017-02-01 19:07:30.333492'),
                                            send_failures=['blah2@blah.com', 'blah4@blah.com'])
            assert TestDataFreshnessStatus.email_users_result == [(['blah2@blah.com', 'blah4@blah.com'],
                                                                   'FAILURE: Future run date!',
                                                                   'Dear system administrator,\n\nIt is highly probable that data freshness has failed!\n\nBest wishes,\nHDX Team',
                                                                   '<html>\n  <head></head>\n  <body>\n    <span>Dear system administrator,<br><br>It is highly probable that data freshness has failed!<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n')]
            TestDataFreshnessStatus.email_users_result = list()
            now = parser.parse('2017-02-04 19:07:30.333492')
            databasequeries = DatabaseQueries(session=session, now=now)
            freshness = DataFreshnessStatus(datasethelper=datasethelper, databasequeries=databasequeries, email=email,
                                            sheet=sheet)
            freshness.check_number_datasets(now, send_failures=['blah2@blah.com', 'blah4@blah.com'])
            assert TestDataFreshnessStatus.email_users_result == [(['blah2@blah.com', 'blah4@blah.com'],
                                                                   'FAILURE: No run today!',
                                                                   'Dear system administrator,\n\nIt is highly probable that data freshness has failed!\n\nBest wishes,\nHDX Team',
                                                                   '<html>\n  <head></head>\n  <body>\n    <span>Dear system administrator,<br><br>It is highly probable that data freshness has failed!<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n')]

    def test_freshnessdatasetsnoresources(self, configuration, database_noresources, users, organizations):
        site_url = 'http://lala/'
        sysadmins_to_email = ['blah2@blah.com', 'blah4@blah.com']
        now = parser.parse('2017-02-03 19:07:30.333492')
        sheet = Sheet(now)
        email = Email(send_emails=self.email_users)
        with Database(**database_noresources) as session:
            datasethelper = DatasetHelper(site_url=site_url, users=users, organizations=organizations,
                                          sysadmins_to_email=sysadmins_to_email)
            databasequeries = DatabaseQueries(session=session, now=now)
            freshness = DataFreshnessStatus(datasethelper=datasethelper, databasequeries=databasequeries, email=email,
                                            sheet=sheet)
            sheet.spreadsheet = TestDataFreshnessStatus.TestSpreadsheet_NoResources
            sheet.dutyofficer = 'Andrew'

            TestDataFreshnessStatus.email_users_result = list()
            TestDataFreshnessStatus.cells_result = None
            freshness.process_datasets_noresources()
            assert TestDataFreshnessStatus.email_users_result == \
                   [(['blah2@blah.com', 'blah4@blah.com'], 'Datasets with no resources',
                     'Dear system administrator,\n\nThe following datasets have no resources:\n\nAirports in Samoa (http://lala/dataset/ourairports-wsm) from OurAirports with missing maintainer and organization administrators blah3disp (blah3@blah.com), blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: every year\n\nBest wishes,\nHDX Team',
                     '<html>\n  <head></head>\n  <body>\n    <span>Dear system administrator,<br><br>The following datasets have no resources:<br><br><a href="http://lala/dataset/ourairports-wsm">Airports in Samoa</a> from OurAirports with missing maintainer and organization administrators <a href="mailto:blah3@blah.com">blah3disp</a>, <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: every year<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n')]
            assert TestDataFreshnessStatus.cells_result == [
                ['URL', 'Title', 'Organisation', 'Maintainer', 'Maintainer Email',
                 'Org Admins', 'Org Admin Emails', 'Update Frequency', 'Latest of Modifieds',
                 'Date Added', 'No. Times', 'Assigned', 'Status'],
                ['http://lala/dataset/ourairports-wsm', 'Airports in Samoa',
                 'OurAirports', '', '', 'blah3disp,blah4disp,blah5full',
                 'blah3@blah.com,blah4@blah.com,blah5@blah.com', 'every year',
                 '2015-11-24T23:32:30.661408', '2017-02-03T19:07:30.333492',
                 1, 'Andrew', ''],
                ['http://lala/dataset/ourairports-myt',
                 'Airports in Mayotte', 'OurAirports', 'blah5full', 'blah5@blah.com',
                 'blah3disp,blah4disp,blah5full', 'blah3@blah.com,blah4@blah.com,blah5@blah.com',
                 'every year', '2015-11-24T23:32:32.025059',
                 '2017-01-01T19:07:30.333492', 2, 'Peter', 'Done']]

    def test_dataset_date(self, configuration, database_datasets_modified_yesterday, users, organizations):
        site_url = 'http://lala/'
        sysadmins_to_email = ['blah3@blah.com']
        now = parser.parse('2017-02-02 19:07:30.333492')
        sheet = Sheet(now)
        email = Email(send_emails=self.email_users)
        with Database(**database_datasets_modified_yesterday) as session:
            datasethelper = DatasetHelper(site_url=site_url, users=users, organizations=organizations,
                                          sysadmins_to_email=sysadmins_to_email)
            databasequeries = DatabaseQueries(session=session, now=now)
            freshness = DataFreshnessStatus(datasethelper=datasethelper, databasequeries=databasequeries, email=email,
                                            sheet=sheet)
            sheet.spreadsheet = TestDataFreshnessStatus.TestSpreadsheet_Empty
            sheet.dutyofficer = 'Sharon'

            TestDataFreshnessStatus.email_users_result = list()
            TestDataFreshnessStatus.cells_result = None
            freshness.process_datasets_dataset_date()
            expected_result = \
                [([{'email': 'blah4@blah.com', 'id': 'blah4', 'name': 'blah4name', 'sysadmin': True,
                    'fullname': 'blah4full', 'display_name': 'blah4disp'}],
                  'Check date of dataset for your datasets on HDX',
                  'Dear blah4disp,\n\nThe dataset(s) listed below have a date of dataset that has not been updated for a while. Log into the HDX platform now to check and if necessary update each dataset.\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with expected update frequency: every year and date of dataset: 11/01/2015\n\nBest wishes,\nHDX Team',
                  '<html>\n  <head></head>\n  <body>\n    <span>Dear blah4disp,<br><br>The dataset(s) listed below have a date of dataset that has not been updated for a while. Log into the HDX platform now to check and if necessary update each dataset.<br><br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with expected update frequency: every year and date of dataset: 11/01/2015<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n'),
                 ([{'email': 'blah5@blah.com', 'id': 'blah5', 'name': 'blah5name', 'sysadmin': False,
                    'fullname': 'blah5full'}], 'Check date of dataset for your datasets on HDX',
                  'Dear blah5full,\n\nThe dataset(s) listed below have a date of dataset that has not been updated for a while. Log into the HDX platform now to check and if necessary update each dataset.\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with expected update frequency: every year and date of dataset: 11/01/2015\n\nBest wishes,\nHDX Team',
                  '<html>\n  <head></head>\n  <body>\n    <span>Dear blah5full,<br><br>The dataset(s) listed below have a date of dataset that has not been updated for a while. Log into the HDX platform now to check and if necessary update each dataset.<br><br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with expected update frequency: every year and date of dataset: 11/01/2015<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n')]
            expected_cells_result = \
                [['URL', 'Title', 'Organisation', 'Maintainer', 'Maintainer Email', 'Org Admins', 'Org Admin Emails',
                  'Dataset Date', 'Update Frequency', 'Latest of Modifieds', 'Date Added', 'No. Times', 'Assigned',
                  'Status'],
                 ['http://lala/dataset/yemen-admin-boundaries', 'Yemen - Administrative Boundaries', 'OCHA Yemen',
                  '', '',
                  'blah4disp,blah5full', 'blah4@blah.com,blah5@blah.com', '', 'every year',
                  '2017-02-02T10:07:30.333492', '2017-02-02T19:07:30.333492', 1, 'Sharon', '']]
            assert TestDataFreshnessStatus.email_users_result == expected_result
            assert TestDataFreshnessStatus.cells_result == expected_cells_result
            TestDataFreshnessStatus.email_users_result = list()
            TestDataFreshnessStatus.cells_result = None
            mikeuser = User(
                {'email': 'mcarans@yahoo.co.uk', 'name': 'mcarans', 'sysadmin': True, 'fullname': 'Michael Rans',
                 'display_name': 'Michael Rans'})
            freshness.process_datasets_dataset_date(sysadmins=[mikeuser])
            expected_result.append(
                ([{'email': 'mcarans@yahoo.co.uk', 'name': 'mcarans', 'sysadmin': True,
                   'fullname': 'Michael Rans', 'display_name': 'Michael Rans'}], 'All date of dataset emails',
                 'Dear system administrator,\n\nDear blah4disp,\n\nThe dataset(s) listed below have a date of dataset that has not been updated for a while. Log into the HDX platform now to check and if necessary update each dataset.\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with expected update frequency: every year and date of dataset: 11/01/2015\nDear blah5full,\n\nThe dataset(s) listed below have a date of dataset that has not been updated for a while. Log into the HDX platform now to check and if necessary update each dataset.\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with expected update frequency: every year and date of dataset: 11/01/2015\n\nBest wishes,\nHDX Team',
                 '<html>\n  <head></head>\n  <body>\n    <span>Dear system administrator,<br><br>Dear blah4disp,<br><br>The dataset(s) listed below have a date of dataset that has not been updated for a while. Log into the HDX platform now to check and if necessary update each dataset.<br><br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with expected update frequency: every year and date of dataset: 11/01/2015<br>Dear blah5full,<br><br>The dataset(s) listed below have a date of dataset that has not been updated for a while. Log into the HDX platform now to check and if necessary update each dataset.<br><br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with expected update frequency: every year and date of dataset: 11/01/2015<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n'))
            assert TestDataFreshnessStatus.email_users_result == expected_result
            assert TestDataFreshnessStatus.cells_result == expected_cells_result
            TestDataFreshnessStatus.email_users_result = list()
            TestDataFreshnessStatus.cells_result = None

    def test_datasets_datagrid(self, configuration, database_datasets_modified_yesterday, users, organizations):
        project_config_yaml = join('tests', 'fixtures', 'project_configuration.yml')
        configuration = Configuration(hdx_site='prod', user_agent='test', hdx_read_only=True,
                                      project_config_yaml=project_config_yaml)
        site_url = 'http://lala/'
        sysadmins_to_email = ['blah3@blah.com']
        now = parser.parse('2017-02-02 19:07:30.333492')
        sheet = Sheet(now)
        sheet.setup_input(configuration)
        email = Email(send_emails=self.email_users)
        with Database(**database_datasets_modified_yesterday) as session:
            datasethelper = DatasetHelper(site_url=site_url, users=users, organizations=organizations,
                                          sysadmins_to_email=sysadmins_to_email)
            databasequeries = DatabaseQueries(session=session, now=now)
            freshness = DataFreshnessStatus(datasethelper=datasethelper, databasequeries=databasequeries, email=email,
                                            sheet=sheet)
            sheet.spreadsheet = TestDataFreshnessStatus.TestSpreadsheet_Empty
            sheet.dutyofficer = 'Sharon'

            TestDataFreshnessStatus.email_users_result = list()
            freshness.process_datasets_datagrid(datasetclass=self.TestDataset)
            expected_result = \
                [([{'fullname': 'Godfrey', 'email': 'godfrey@abc.org'}, {'fullname': 'Peter', 'email': 'pete@abc.org'}],
                  'Candidates for the datagrid',
                  'Dear system administrator,\n\nThe new dataset(s) listed below are candidates for the data grid:\n\nAirports in Samoa (http://lala/dataset/ourairports-wsm) from OurAirports with missing maintainer and organization administrators blah3disp (blah3@blah.com), blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: every year\n\nBest wishes,\nHDX Team',
                  '<html>\n  <head></head>\n  <body>\n    <span>Dear system administrator,<br><br>The new dataset(s) listed below are candidates for the data grid:<br><br><a href="http://lala/dataset/ourairports-wsm">Airports in Samoa</a> from OurAirports with missing maintainer and organization administrators <a href="mailto:blah3@blah.com">blah3disp</a>, <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: every year<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n'),
                 ([{'fullname': 'Godfrey', 'email': 'godfrey@abc.org'}], 'Candidates for the datagrid',
                  'Dear system administrator,\n\nThe new dataset(s) listed below are candidates for the data grid:\n\nAirports in Samoa (http://lala/dataset/ourairports-wsm) from OurAirports with missing maintainer and organization administrators blah3disp (blah3@blah.com), blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: every year\n\nBest wishes,\nHDX Team',
                  '<html>\n  <head></head>\n  <body>\n    <span>Dear system administrator,<br><br>The new dataset(s) listed below are candidates for the data grid:<br><br><a href="http://lala/dataset/ourairports-wsm">Airports in Samoa</a> from OurAirports with missing maintainer and organization administrators <a href="mailto:blah3@blah.com">blah3disp</a>, <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: every year<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n'),
                 ([{'fullname': 'Peter', 'email': 'pete@abc.org'}], 'Candidates for the datagrid',
                  'Dear system administrator,\n\nThe new dataset(s) listed below are candidates for the data grid:\n\nAirports in Samoa (http://lala/dataset/ourairports-wsm) from OurAirports with missing maintainer and organization administrators blah3disp (blah3@blah.com), blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: every year\n\nBest wishes,\nHDX Team',
                  '<html>\n  <head></head>\n  <body>\n    <span>Dear system administrator,<br><br>The new dataset(s) listed below are candidates for the data grid:<br><br><a href="http://lala/dataset/ourairports-wsm">Airports in Samoa</a> from OurAirports with missing maintainer and organization administrators <a href="mailto:blah3@blah.com">blah3disp</a>, <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: every year<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n')]
            expected_cells_result = \
                [['URL', 'Title', 'Organisation', 'Maintainer', 'Maintainer Email', 'Org Admins', 'Org Admin Emails',
                  'Dataset Date', 'Update Frequency', 'Latest of Modifieds', 'Date Added', 'No. Times', 'Assigned',
                  'Status'],
                 ['http://lala/dataset/ourairports-wsm', 'Airports in Samoa', 'OurAirports', '', '',
                  'blah3disp,blah4disp,blah5full',
                  'blah3@blah.com,blah4@blah.com,blah5@blah.com', '', 'every year', '2017-02-02T10:07:30.333492',
                  '2017-02-02T19:07:30.333492', 1, 'Peter', '']]
            assert TestDataFreshnessStatus.email_users_result == expected_result
            assert TestDataFreshnessStatus.cells_result == expected_cells_result
            TestDataFreshnessStatus.email_users_result = list()
            TestDataFreshnessStatus.cells_result = None
