# -*- coding: utf-8 -*-
"""
Sheet
-----

Utilities to handle interaction with Google sheets
"""
import json
import logging
from datetime import datetime

import hxl
import pygsheets
from google.oauth2 import service_account

logger = logging.getLogger(__name__)


def get_date(datestr):
    return datetime.strptime(datestr, '%Y-%m-%d')


class Sheet:
    def __init__(self, now):
        self.now = now
        self.spreadsheet = None
        self.dutyofficer = None

    def setup_input(self, configuration):
        logger.info('--------------------------------------------------')
        try:
            dutyofficers = hxl.data(configuration['duty_roster_url']).with_columns(['#date+start', '#contact+name'])
            dutyofficers = dutyofficers.sort(keys=['#date+start'], reverse=True)

            for dutyofficer in dutyofficers:
                if datetime.strptime(dutyofficer.get('#date+start'), '%Y-%m-%d') <= self.now:
                    self.dutyofficer = dutyofficer.get('#contact+name')
                    logger.info('Duty officer: %s' % self.dutyofficer)
                    break
        except Exception as ex:
            return str(ex)

    def setup_output(self, configuration, gsheet_auth):
        if gsheet_auth:
            try:
                logger.info('> GSheet Credentials: %s' % gsheet_auth)
                info = json.loads(gsheet_auth)
                scopes = ['https://www.googleapis.com/auth/spreadsheets']
                credentials = service_account.Credentials.from_service_account_info(info, scopes=scopes)
                gc = pygsheets.authorize(custom_credentials=credentials)
                self.spreadsheet = gc.open_by_url(configuration['issues_spreadsheet_url'])
            except Exception as ex:
                return str(ex)
        else:
            logger.info('> No GSheet Credentials!')
            self.spreadsheet = None
        return None

    def update(self, sheetname, datasets):
        # sheet must have been set up!
        if self.spreadsheet is None or self.dutyofficer is None:
            logger.warning('Cannot update Google spreadsheet!')
            return
        logger.info('Updating Google spreadsheet.')
        sheet = self.spreadsheet.worksheet_by_title(sheetname)
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
                new_row[assigned_ind] = self.dutyofficer
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
