# -*- coding: utf-8 -*-
"""
Sheet
-----

Utilities to handle interaction with Google sheets
"""
import json
import logging
from datetime import datetime

import pygsheets
from google.oauth2 import service_account

logger = logging.getLogger(__name__)


def get_date(datestr):
    return datetime.strptime(datestr, '%Y-%m-%d')


class Sheet:
    def __init__(self, now):
        self.now = now
        self.dutyofficers_spreadsheet = None
        self.datagrids_spreadsheet = None
        self.issues_spreadsheet = None
        self.dutyofficer = None
        self.datagrids = dict()
        self.datagridccs = list()

    @staticmethod
    def add_query(hxltags, grid, row):
        category = row[hxltags['#category']].strip()
        include = row[hxltags['#include']]
        if include:
            include = include.strip()
        exclude = row[hxltags['#exclude']]
        if exclude:
            exclude = exclude.strip()
        query = grid.get(category, '')
        if query:
            queryparts = query.split(' ! ')
            query = queryparts[0]
            if include:
                query = '%s OR %s' % (query, include)
            if len(queryparts) > 1:
                query = '%s ! %s' % (query, ' ! '.join(queryparts[1:]))
        elif include:
            query = include
        if exclude:
            query = '%s ! %s' % (query, exclude)
        grid[category] = query

    def get_datagrid(self, hxltags, dg, datagrids, defaultgrid):
        datagridname = dg.strip()
        if datagridname == '' or datagridname == 'cc':
            return None
        datagrid = self.datagrids.get(datagridname)
        if datagrid is None:
            datagrid = dict()
            self.datagrids[datagridname] = datagrid
            for row in datagrids:
                if row[hxltags['#datagrid']] == datagridname:
                    self.add_query(hxltags, datagrid, row)
            for key in defaultgrid:
                if key not in datagrid:
                    if key == 'datagrid':
                        datagrid[key] = defaultgrid[key].replace('$datagrid', datagridname)
                    else:
                        datagrid[key] = defaultgrid[key]
        return datagrid

    def setup_gsheet(self, configuration, gsheet_auth, spreadsheet_test, no_spreadsheet):
        if not gsheet_auth:
            return 'No GSheet Credentials!'
        try:
            info = json.loads(gsheet_auth)
            scopes = ['https://www.googleapis.com/auth/spreadsheets']
            credentials = service_account.Credentials.from_service_account_info(info, scopes=scopes)
            gc = pygsheets.authorize(custom_credentials=credentials)
            if spreadsheet_test:  # use test not prod spreadsheet
                issues_spreadsheet = configuration['test_issues_spreadsheet_url']
            else:
                issues_spreadsheet = configuration['prod_issues_spreadsheet_url']
            logger.info('Opening duty officers gsheet')
            self.dutyofficers_spreadsheet = gc.open_by_url(configuration['dutyofficers_url'])
            logger.info('Opening datagrids gsheet')
            self.datagrids_spreadsheet = gc.open_by_url(configuration['datagrids_url'])
            if not no_spreadsheet:
                logger.info('Opening issues gsheet')
                self.issues_spreadsheet = gc.open_by_url(issues_spreadsheet)
            else:
                self.issues_spreadsheet = None
        except Exception as ex:
            return str(ex)
        return None

    def setup_input(self):
        logger.info('--------------------------------------------------')
        try:
            sheet = self.dutyofficers_spreadsheet.worksheet_by_title('DutyRoster')
            current_values = sheet.get_all_values(returnas='matrix')
            hxltags = {tag: i for i, tag in enumerate(current_values[1])}
            startdate_ind = hxltags['#date+start']
            contactname_ind = hxltags['#contact+name']
            contactemail_ind = hxltags['#contact+email']
            for row in sorted(current_values[2:], key=lambda x: x[startdate_ind], reverse=True):
                startdate = row[startdate_ind].strip()
                if datetime.strptime(startdate, '%Y-%m-%d') <= self.now:
                    dutyofficer_name = row[contactname_ind]
                    if dutyofficer_name:
                        dutyofficer_name = dutyofficer_name.strip()
                        self.dutyofficer = {'name': dutyofficer_name, 'email': row[contactemail_ind].strip()}
                        logger.info('Duty officer: %s' % dutyofficer_name)
                        break

            sheet = self.datagrids_spreadsheet.worksheet_by_title('DataGrids')
            current_values = sheet.get_all_values(returnas='matrix')
            hxltags = {tag: i for i, tag in enumerate(current_values[1])}
            datagrids = current_values[2:]
            defaultgrid = dict()
            for row in datagrids:
                if row[hxltags['#datagrid']] == 'default':
                    self.add_query(hxltags, defaultgrid, row)

            sheet = self.datagrids_spreadsheet.worksheet_by_title('Curators')
            current_values = sheet.get_all_values(returnas='matrix')
            curators_hxltags = {tag: i for i, tag in enumerate(current_values[1])}
            curators = current_values[2:]
            for row in curators:
                curatoremail = row[curators_hxltags['#contact+email']].strip()
                owner = row[curators_hxltags['#datagrid']]
                for dg in owner.strip().split(','):
                    if dg.strip() == 'cc':
                        self.datagridccs.append(curatoremail)
            for row in curators:
                curatorname = row[curators_hxltags['#contact+name']].strip()
                curatoremail = row[curators_hxltags['#contact+email']].strip()
                owner = row[curators_hxltags['#datagrid']]
                if owner is not None:
                    for dg in owner.strip().split(','):
                        datagrid = self.get_datagrid(hxltags, dg, datagrids, defaultgrid)
                        if datagrid is None:
                            continue
                        if datagrid.get('owner'):
                            raise ValueError('There is more than one owner of datagrid %s!' % dg)
                        datagrid['owner'] = {'name': curatorname, 'email': curatoremail}
            for datagridname in self.datagrids:
                if 'owner' not in self.datagrids[datagridname]:
                    raise ValueError('Datagrid %s does not have an owner!' % datagridname)
        except Exception as ex:
            return str(ex)

    def update(self, sheetname, datasets, dutyofficer_name=None):
        # sheet must have been set up!
        if self.issues_spreadsheet is None or (self.dutyofficer is None and dutyofficer_name is None):
            logger.warning('Cannot update Google spreadsheet!')
            return
        logger.info('Updating Google spreadsheet.')
        sheet = self.issues_spreadsheet.worksheet_by_title(sheetname)
        current_values = sheet.get_all_values(returnas='matrix')
        keys = current_values[0]
        url_ind = keys.index('URL')
        dateadded_ind = keys.index('Date Added')
        no_times_ind = keys.index('No. Times')
        assigned_ind = keys.index('Assigned')
        status_ind = keys.index('Status')
        urls = [x[url_ind] for i, x in enumerate(current_values) if i != 0]
        updated_notimes = set()
        for dataset in datasets:
            url = dataset['URL']
            new_row = [dataset.get(key, '') for key in keys]
            try:
                rowno = urls.index(url) + 1
                current_row = current_values[rowno]
                new_row[dateadded_ind] = current_row[dateadded_ind]
                no_times = current_row[no_times_ind]
                new_row[no_times_ind] = int(no_times)
                if url not in updated_notimes:
                    updated_notimes.add(url)
                    new_row[no_times_ind] += 1
                new_row[assigned_ind] = current_row[assigned_ind]
                new_row[status_ind] = current_row[status_ind]
                current_values[rowno] = new_row
            except ValueError:
                new_row[dateadded_ind] = self.now.isoformat()
                new_row[no_times_ind] = 1
                if dutyofficer_name is not None:
                    new_row[assigned_ind] = dutyofficer_name
                else:
                    new_row[assigned_ind] = self.dutyofficer['name']
                current_values.append(new_row)
                urls.append(url)
                updated_notimes.add(url)
        current_values = sorted(current_values, key=lambda x: x[dateadded_ind], reverse=True)
        sheet.update_values('A1', current_values)

    @staticmethod
    def construct_row(datasethelper, dataset, maintainer, orgadmins):
        url = datasethelper.get_dataset_url(dataset)
        title = dataset['title']
        org_title = dataset['organization_title']
        if maintainer:
            maintainer_name, maintainer_email = maintainer
        else:
            maintainer_name, maintainer_email = '', ''
        orgadmin_names = ','.join([x[0] for x in orgadmins])
        orgadmin_emails = ','.join([x[1] for x in orgadmins])
        update_freq = datasethelper.get_update_frequency(dataset)
        latest_of_modifieds = dataset['latest_of_modifieds'].isoformat()
        # URL	Title	Organisation	Maintainer	Maintainer Email	Org Admins	Org Admin Emails
        # Update Frequency	Latest of Modifieds
        row = {'URL': url, 'Title': title, 'Organisation': org_title,
               'Maintainer': maintainer_name, 'Maintainer Email': maintainer_email,
               'Org Admins': orgadmin_names, 'Org Admin Emails': orgadmin_emails,
               'Update Frequency': update_freq, 'Latest of Modifieds': latest_of_modifieds}
        return row
