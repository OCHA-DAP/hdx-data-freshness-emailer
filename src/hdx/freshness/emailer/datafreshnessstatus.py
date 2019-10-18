# -*- coding: utf-8 -*-
"""
Data Freshness Status
---------------------

Determines freshness status
"""


import datetime
import logging

from hdx.data.dataset import Dataset
from hdx.data.organization import Organization
from hdx.data.user import User
from hdx.hdx_configuration import Configuration
from hdx.utilities.dictandlist import dict_of_lists_add

from hdx.freshness.emailer.freshnessemail import Email
from hdx.freshness.emailer.utilities import get_dataset_dates

logger = logging.getLogger(__name__)


class DataFreshnessStatus:
    freshness_status = {0: 'Fresh', 1: 'Due', 2: 'Overdue', 3: 'Delinquent'}
    object_output_limit = 2

    def __init__(self, site_url, databasequeries, email, sheet, users=None, organizations=None,
                 ignore_sysadmin_emails=None):
        ''''''
        self.site_url = site_url
        self.databasequeries = databasequeries
        if users is None:  # pragma: no cover
            users = User.get_all_users()
        self.email = email
        self.sheet = sheet
        if organizations is None:  # pragma: no cover
            organizations = Organization.get_all_organization_names(all_fields=True, include_users=True)
        if ignore_sysadmin_emails is None:  # pragma: no cover
            ignore_sysadmin_emails = Configuration.read()['ignore_sysadmin_emails']
        self.users = dict()
        self.sysadmins = dict()
        self.sysadmins_to_email = list()
        for user in users:
            userid = user['id']
            self.users[userid] = user
            if user['sysadmin']:
                self.sysadmins[userid] = user
                if user['fullname'] and user['email'] not in ignore_sysadmin_emails:
                    self.sysadmins_to_email.append(user)
        self.organizations = dict()
        for organization in organizations:
            users_per_capacity = dict()
            for user in organization['users']:
                dict_of_lists_add(users_per_capacity, user['capacity'], user['id'])
            self.organizations[organization['id']] = users_per_capacity

    def get_maintainer(self, dataset):
        maintainer = dataset['maintainer']
        return self.users.get(maintainer)

    def get_org_admins(self, dataset):
        organization_id = dataset['organization_id']
        orgadmins = list()
        organization = self.organizations[organization_id]
        if 'admin' in organization:
            for userid in self.organizations[organization_id]['admin']:
                user = self.users.get(userid)
                if user:
                    orgadmins.append(user)
        return orgadmins

    def get_maintainer_orgadmins(self, dataset):
        users_to_email = list()
        maintainer = self.get_maintainer(dataset)
        if maintainer is not None:
            users_to_email.append(maintainer)
            maintainer_name = self.get_user_name(maintainer)
            maintainer = (maintainer_name, maintainer['email'])
        orgadmins = list()
        for orgadmin in self.get_org_admins(dataset):
            if maintainer is None:
                users_to_email.append(orgadmin)
            username = self.get_user_name(orgadmin)
            orgadmins.append((username, orgadmin['email']))
        return maintainer, orgadmins, users_to_email

    @staticmethod
    def get_update_frequency(dataset):
        if dataset['update_frequency'] is None:
            return 'NOT SET'
        else:
            return Dataset.transform_update_frequency('%d' % dataset['update_frequency']).lower()

    @staticmethod
    def get_user_name(user):
        user_name = user.get('display_name')
        if not user_name:
            user_name = user['fullname']
            if not user_name:
                user_name = user['name']
        return user_name

    def get_dataset_url(self, dataset):
        return '%sdataset/%s' % (self.site_url, dataset['name'])

    def get_organization_url(self, organization):
        return '%sorganization/%s' % (self.site_url, organization['name'])

    def create_dataset_string(self, dataset, maintainer, orgadmins, sysadmin=False, include_org=True,
                              include_freshness=False, include_datasetdate=False):
        url = self.get_dataset_url(dataset)
        msg = list()
        htmlmsg = list()
        msg.append('%s (%s)' % (dataset['title'], url))
        htmlmsg.append('<a href="%s">%s</a>' % (url, dataset['title']))
        if sysadmin and include_org:
            orgmsg = ' from %s' % dataset['organization_title']
            msg.append(orgmsg)
            htmlmsg.append(orgmsg)
        if maintainer is not None:
            if sysadmin:
                user_name, user_email = maintainer
                msg.append(' maintained by %s (%s)' % (user_name, user_email))
                htmlmsg.append(' maintained by <a href="mailto:%s">%s</a>' % (user_email, user_name))
        else:
            if sysadmin:
                missing_maintainer = ' with missing maintainer and organization administrators '
                msg.append(missing_maintainer)
                htmlmsg.append(missing_maintainer)

            usermsg = list()
            userhtmlmsg = list()
            for orgadmin in orgadmins:
                user_name, user_email = orgadmin
                usermsg.append('%s (%s)' % (user_name, user_email))
                userhtmlmsg.append('<a href="mailto:%s">%s</a>' % (user_email, user_name))
            if sysadmin:
                msg.append(', '.join(usermsg))
                htmlmsg.append(', '.join(userhtmlmsg))
        update_frequency = self.get_update_frequency(dataset)
        msg.append(' with expected update frequency: %s' % update_frequency)
        htmlmsg.append(' with expected update frequency: %s' % update_frequency)
        if include_freshness:
            fresh = self.freshness_status.get(dataset['fresh'], 'None')
            msg.append(' and freshness: %s' % fresh)
            htmlmsg.append(' and freshness: %s' % fresh)
        if include_datasetdate:
            datasetdate = dataset['dataset_date']
            msg.append(' and date of dataset: %s' % datasetdate)
            htmlmsg.append(' and date of dataset: %s' % datasetdate)
        Email.output_newline(msg, htmlmsg)

        return ''.join(msg), ''.join(htmlmsg)

    def check_number_datasets(self, now, send_failures=None):
        logger.info('\n\n*** Checking number of datasets ***')
        run_numbers = self.databasequeries.get_run_numbers()
        run_date = run_numbers[0][1]
        stop = True
        if now < run_date:
            title = 'FAILURE: Future run date!'
            msg = 'Dear system administrator,\n\nIt is highly probable that data freshness has failed!\n'
            send_to = send_failures
        elif now - run_date > datetime.timedelta(days=1):
            title = 'FAILURE: No run today!'
            msg = 'Dear system administrator,\n\nIt is highly probable that data freshness has failed!\n'
            send_to = send_failures
        elif len(run_numbers) == 2:
            datasets_today, datasets_previous = self.databasequeries.get_number_datasets()
            diff_datasets = datasets_previous - datasets_today
            percentage_diff = diff_datasets / datasets_previous
            if percentage_diff <= 0.02:
                logger.info('No issues with number of datasets.')
                return False
            if percentage_diff == 1.0:
                title = 'FAILURE: No datasets today!'
                msg = 'Dear system administrator,\n\nIt is highly probable that data freshness has failed!\n'
                send_to = send_failures
            else:
                title = 'WARNING: Fall in datasets on HDX today!'
                msg = 'Dear system administrator,\n\nThere are %d (%d%%) fewer datasets today than yesterday on HDX!\n' % \
                      (diff_datasets, percentage_diff * 100)
                send_to = self.sysadmins_to_email
                stop = False
        else:
            logger.info('No issues with number of datasets.')
            return False
        self.email.htmlify_send(send_to, title, msg)
        return stop

    def send_broken_email(self, sendto=None):
        datasets_flat = list()
        datasets = self.databasequeries.get_broken()
        if len(datasets) == 0:
            logger.info('No broken datasets found.')
            return datasets_flat
        startmsg = 'Dear system administrator,\n\nThe following datasets have broken resources:\n\n'
        msg = [startmsg]
        htmlmsg = [Email.html_start(Email.convert_newlines(startmsg))]

        def output_tabs(n=1):
            for i in range(n):
                msg.append('  ')
                htmlmsg.append('&nbsp&nbsp')

        def create_broken_dataset_string(ds, ma, oa):
            dataset_string, dataset_html_string = \
                self.create_dataset_string(ds, ma, oa, sysadmin=True, include_org=False, include_freshness=True)
            output_tabs(2)
            msg.append(dataset_string)
            htmlmsg.append(dataset_html_string)
            newline = False
            for i, resource in enumerate(sorted(ds['resources'], key=lambda d: d['name'])):
                if i == self.object_output_limit:
                    output_tabs(3)
                if i >= self.object_output_limit:
                    newline = True
                    output_tabs(1)
                    msg.append('%s (%s)' % (resource['name'], resource['id']))
                    htmlmsg.append('%s (%s)' % (resource['name'], resource['id']))
                    continue
                resource_string = 'Resource %s (%s) has error: %s!' % \
                                  (resource['name'], resource['id'], resource['error'])
                output_tabs(4)
                msg.append('%s\n' % resource_string)
                htmlmsg.append('%s<br>' % resource_string)
            if newline:
                Email.output_newline(msg, htmlmsg)

        def create_cut_down_broken_dataset_string(i, ds):
            if i == self.object_output_limit:
                output_tabs(1)
            if i >= self.object_output_limit:
                url = self.get_dataset_url(ds)
                output_tabs(1)
                msg.append('%s (%s)' % (ds['title'], url))
                htmlmsg.append('<a href="%s">%s</a>' % (url, ds['title']))
                return True
            return False

        def output_error(error):
            msg.append(error)
            htmlmsg.append('<b>%s</b>' % error)
            Email.output_newline(msg, htmlmsg)

        def output_org(title):
            msg.append(title)
            htmlmsg.append('<b><i>%s</i></b>' % title)
            Email.output_newline(msg, htmlmsg)

        for error_type in sorted(datasets):
            output_error(error_type)
            datasets_error = datasets[error_type]
            for org_title in sorted(datasets_error):
                output_org(org_title)
                org = datasets_error[org_title]
                newline = False
                for i, dataset_name in enumerate(sorted(org)):
                    dataset = org[dataset_name]
                    maintainer, orgadmins, _ = self.get_maintainer_orgadmins(dataset)
                    cut_down = create_cut_down_broken_dataset_string(i, dataset)
                    if cut_down:
                        newline = True
                    else:
                        create_broken_dataset_string(dataset, maintainer, orgadmins)
                    url = self.get_dataset_url(dataset)
                    title = dataset['title']
                    if maintainer:
                        maintainer_name, maintainer_email = maintainer
                    else:
                        maintainer_name, maintainer_email = '', ''
                    orgadmin_names = ','.join([x[0] for x in orgadmins])
                    orgadmin_emails = ','.join([x[1] for x in orgadmins])
                    update_freq = self.get_update_frequency(dataset)
                    latest_of_modifieds = dataset['latest_of_modifieds'].isoformat()
                    fresh = self.freshness_status.get(dataset['fresh'], 'None')
                    error = list()
                    for resource in sorted(dataset['resources'], key=lambda d: d['name']):
                        error.append('%s:%s' % (resource['name'], resource['error']))
                    # Date Added    URL	Title	Organisation	Maintainer	Maintainer Email	Org Admins
                    # Org Admin Emails	Update Frequency    Latest of Modifieds	Freshness	Error Type	Error
                    row = {'URL': url, 'Title': title, 'Organisation': org_title,
                           'Maintainer': maintainer_name, 'Maintainer Email': maintainer_email,
                           'Org Admins': orgadmin_names, 'Org Admin Emails': orgadmin_emails,
                           'Update Frequency': update_freq, 'Latest of Modifieds': latest_of_modifieds,
                           'Freshness': fresh,
                           'Error Type': error_type, 'Error': '\n'.join(error)}
                    datasets_flat.append(row)
                if newline:
                    Email.output_newline(msg, htmlmsg)
            Email.output_newline(msg, htmlmsg)

        if sendto is None:
            users_to_email = self.sysadmins_to_email
        else:
            users_to_email = sendto
        self.email.close_send(users_to_email, 'Broken datasets', msg, htmlmsg)
        return datasets_flat

    def process_broken(self, sendto=None):
        logger.info('\n\n*** Checking for broken datasets ***')
        datasets = self.send_broken_email(sendto=sendto)
        self.sheet.update('Broken', datasets)

    def send_delinquent_email(self):
        datasets_flat = list()
        datasets = self.databasequeries.get_status(3)
        if len(datasets) == 0:
            logger.info('No delinquent datasets found.')
            return datasets_flat
        startmsg = 'Dear system administrator,\n\nThe following datasets have just become delinquent:\n\n'
        msg = [startmsg]
        htmlmsg = [Email.html_start(Email.convert_newlines(startmsg))]
        for dataset in sorted(datasets, key=lambda d: (d['organization_title'], d['name'])):
            maintainer, orgadmins, _ = self.get_maintainer_orgadmins(dataset)
            dataset_string, dataset_html_string = self.create_dataset_string(dataset, maintainer, orgadmins, sysadmin=True)
            msg.append(dataset_string)
            htmlmsg.append(dataset_html_string)
            url = self.get_dataset_url(dataset)
            title = dataset['title']
            org_title = dataset['organization_title']
            if maintainer:
                maintainer_name, maintainer_email = maintainer
            else:
                maintainer_name, maintainer_email = '', ''
            orgadmin_names = ','.join([x[0] for x in orgadmins])
            orgadmin_emails = ','.join([x[1] for x in orgadmins])
            update_freq = self.get_update_frequency(dataset)
            latest_of_modifieds = dataset['latest_of_modifieds'].isoformat()
            # URL	Title	Organisation	Maintainer	Maintainer Email	Org Admins	Org Admin Emails
            # Update Frequency	Latest of Modifieds
            row = {'URL': url, 'Title': title, 'Organisation': org_title,
                   'Maintainer': maintainer_name, 'Maintainer Email': maintainer_email,
                   'Org Admins': orgadmin_names, 'Org Admin Emails': orgadmin_emails,
                   'Update Frequency': update_freq, 'Latest of Modifieds': latest_of_modifieds}
            datasets_flat.append(row)
        self.email.close_send(self.sysadmins_to_email, 'Delinquent datasets', msg, htmlmsg)
        return datasets_flat

    def process_delinquent(self):
        logger.info('\n\n*** Checking for delinquent datasets ***')
        datasets = self.send_delinquent_email()
        self.sheet.update('Delinquent', datasets)

    def send_overdue_emails(self, sendto=None, sysadmins=None):
        datasets = self.databasequeries.get_status(2)
        if len(datasets) == 0:
            logger.info('No overdue datasets found.')
            return
        startmsg = 'Dear %s,\n\nThe dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). Log into the HDX platform now to update each dataset.\n\n'
        starthtmlmsg = Email.html_start(Email.convert_newlines(startmsg))
        all_users_to_email = dict()
        for dataset in sorted(datasets, key=lambda d: (d['organization_title'], d['name'])):
            maintainer, orgadmins, users_to_email = self.get_maintainer_orgadmins(dataset)
            dataset_string, dataset_html_string = self.create_dataset_string(dataset, maintainer, orgadmins)
            for user in users_to_email:
                id = user['id']
                output_list = all_users_to_email.get(id)
                if output_list is None:
                    output_list = list()
                    all_users_to_email[id] = output_list
                output_list.append((dataset_string, dataset_html_string))
        emails = dict()
        for id in sorted(all_users_to_email.keys()):
            user = self.users[id]
            basemsg = startmsg % self.get_user_name(user)
            dict_of_lists_add(emails, 'plain', basemsg)
            dict_of_lists_add(emails, 'html', self.email.convert_newlines(basemsg))
            msg = [basemsg]
            htmlmsg = [starthtmlmsg % self.get_user_name(user)]
            for dataset_string, dataset_html_string in all_users_to_email[id]:
                msg.append(dataset_string)
                htmlmsg.append(dataset_html_string)
                dict_of_lists_add(emails, 'plain', dataset_string)
                dict_of_lists_add(emails, 'html', dataset_html_string)
            endmsg = '\nTip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.\n'
            if sendto is None:
                users_to_email = [user]
            else:
                users_to_email = sendto
            self.email.close_send(users_to_email, 'Time to update your datasets on HDX', msg, htmlmsg, endmsg)
        self.email.send_sysadmin_summary(sysadmins, emails, 'All overdue dataset emails')

    def process_overdue(self, sendto=None, sysadmins=None):
        logger.info('\n\n*** Checking for overdue datasets ***')
        self.send_overdue_emails(sendto=sendto, sysadmins=sysadmins)

    def send_maintainer_email(self, invalid_maintainers):
        datasets_flat = list()
        if len(invalid_maintainers) == 0:
            logger.info('No invalid maintainers found.')
            return datasets_flat
        startmsg = 'Dear system administrator,\n\nThe following datasets have an invalid maintainer:\n\n'
        msg = [startmsg]
        htmlmsg = [Email.html_start(Email.convert_newlines(startmsg))]
        for dataset in sorted(invalid_maintainers, key=lambda d: (d['organization_title'], d['name'])):
            maintainer, orgadmins, _ = self.get_maintainer_orgadmins(dataset)
            dataset_string, dataset_html_string = self.create_dataset_string(dataset, maintainer, orgadmins,
                                                                             sysadmin=True)
            msg.append(dataset_string)
            htmlmsg.append(dataset_html_string)
            url = self.get_dataset_url(dataset)
            title = dataset['title']
            org_title = dataset['organization_title']
            if maintainer:
                maintainer_name, maintainer_email = maintainer
            else:
                maintainer_name, maintainer_email = '', ''
            orgadmin_names = ','.join([x[0] for x in orgadmins])
            orgadmin_emails = ','.join([x[1] for x in orgadmins])
            update_freq = self.get_update_frequency(dataset)
            latest_of_modifieds = dataset['latest_of_modifieds'].isoformat()
            # URL	Title	Organisation	Maintainer	Maintainer Email	Org Admins	Org Admin Emails
            # Update Frequency	Latest of Modifieds
            row = {'URL': url, 'Title': title, 'Organisation': org_title,
                   'Maintainer': maintainer_name, 'Maintainer Email': maintainer_email,
                   'Org Admins': orgadmin_names, 'Org Admin Emails': orgadmin_emails,
                   'Update Frequency': update_freq, 'Latest of Modifieds': latest_of_modifieds}
            datasets_flat.append(row)
        self.email.close_send(self.sysadmins_to_email, 'Datasets with invalid maintainer', msg, htmlmsg)
        return datasets_flat

    def send_orgadmins_email(self, invalid_orgadmins):
        organizations_flat = list()
        if len(invalid_orgadmins) == 0:
            logger.info('No invalid organisation administrators found.')
            return organizations_flat
        startmsg = 'Dear system administrator,\n\nThe following organizations have an invalid admin:\n\n'
        msg = [startmsg]
        htmlmsg = [Email.html_start(Email.convert_newlines(startmsg))]
        for key in sorted(invalid_orgadmins):
            organization = invalid_orgadmins[key]
            url = self.get_organization_url(organization)
            title = organization['title']
            error = organization['error']
            msg.append('%s (%s)' % (title, url))
            htmlmsg.append('<a href="%s">%s</a>' % (url, title))
            msg.append(' with error: %s\n' % error)
            htmlmsg.append(' with error: %s<br>' % error)
            # URL	Title	Problem
            row = {'URL': url, 'Title': title, 'Error': error}
            organizations_flat.append(row)
        self.email.close_send(self.sysadmins_to_email, 'Organizations with invalid admins', msg, htmlmsg)
        return organizations_flat

    def process_maintainer_orgadmins(self):
        logger.info('\n\n*** Checking for invalid maintainers and organisation administrators ***')
        invalid_maintainers, invalid_orgadmins = \
            self.databasequeries.get_invalid_maintainer_orgadmins(self.organizations, self.users, self.sysadmins)
        datasets = self.send_maintainer_email(invalid_maintainers)
        self.sheet.update('Maintainer', datasets)
        datasets = self.send_orgadmins_email(invalid_orgadmins)
        self.sheet.update('OrgAdmins', datasets)

    def send_datasets_noresources_email(self):
        datasets_flat = list()
        datasets = self.databasequeries.get_datasets_noresources()
        if len(datasets) == 0:
            logger.info('No datasets with no resources found.')
            return datasets_flat
        startmsg = 'Dear system administrator,\n\nThe following datasets have no resources:\n\n'
        msg = [startmsg]
        htmlmsg = [Email.html_start(Email.convert_newlines(startmsg))]
        for dataset in datasets:
            maintainer, orgadmins, _ = self.get_maintainer_orgadmins(dataset)
            dataset_string, dataset_html_string = self.create_dataset_string(dataset, maintainer, orgadmins,
                                                                             sysadmin=True)
            msg.append(dataset_string)
            htmlmsg.append(dataset_html_string)
            url = self.get_dataset_url(dataset)
            title = dataset['title']
            org_title = dataset['organization_title']
            if maintainer:
                maintainer_name, maintainer_email = maintainer
            else:
                maintainer_name, maintainer_email = '', ''
            orgadmin_names = ','.join([x[0] for x in orgadmins])
            orgadmin_emails = ','.join([x[1] for x in orgadmins])
            update_freq = self.get_update_frequency(dataset)
            latest_of_modifieds = dataset['latest_of_modifieds'].isoformat()
            # URL	Title	Organisation	Maintainer	Maintainer Email	Org Admins	Org Admin Emails
            # Update Frequency	Latest of Modifieds
            row = {'URL': url, 'Title': title, 'Organisation': org_title,
                   'Maintainer': maintainer_name, 'Maintainer Email': maintainer_email,
                   'Org Admins': orgadmin_names, 'Org Admin Emails': orgadmin_emails,
                   'Update Frequency': update_freq, 'Latest of Modifieds': latest_of_modifieds}
            datasets_flat.append(row)
        self.email.close_send(self.sysadmins_to_email, 'Datasets with no resources', msg, htmlmsg)
        return datasets_flat

    def process_datasets_noresources(self):
        logger.info('\n\n*** Checking for datasets with no resources ***')
        datasets = self.send_datasets_noresources_email()
        self.sheet.update('NoResources', datasets)

    def send_datasets_dataset_date_email(self, sendto=None, sysadmins=None):
        datasets_flat = list()
        datasets = self.databasequeries.get_datasets_dataset_date()
        if len(datasets) == 0:
            logger.info('No datasets with date of dataset needing update found.')
            return datasets_flat
        startmsg = 'Dear %s,\n\nThe dataset(s) listed below have a date of dataset that has not been updated for a while. Log into the HDX platform now to check and if necessary update each dataset.\n\n'
        starthtmlmsg = Email.html_start(Email.convert_newlines(startmsg))
        all_users_to_email = dict()
        for dataset in sorted(datasets, key=lambda d: (d['organization_title'], d['name'])):
            maintainer, orgadmins, users_to_email = self.get_maintainer_orgadmins(dataset)
            dataset_string, dataset_html_string = self.create_dataset_string(dataset, maintainer, orgadmins,
                                                                             include_datasetdate=True)
            for user in users_to_email:
                id = user['id']
                output_list = all_users_to_email.get(id)
                if output_list is None:
                    output_list = list()
                    all_users_to_email[id] = output_list
                output_list.append((dataset_string, dataset_html_string))
            url = self.get_dataset_url(dataset)
            title = dataset['title']
            org_title = dataset['organization_title']
            if maintainer:
                maintainer_name, maintainer_email = maintainer
            else:
                maintainer_name, maintainer_email = '', ''
            orgadmin_names = ','.join([x[0] for x in orgadmins])
            orgadmin_emails = ','.join([x[1] for x in orgadmins])
            start_date, end_date = get_dataset_dates(dataset)
            start_date = start_date.isoformat()
            end_date = end_date.isoformat()
            update_freq = self.get_update_frequency(dataset)
            latest_of_modifieds = dataset['latest_of_modifieds'].isoformat()
            # URL	Title	Organisation	Maintainer	Maintainer Email	Org Admins	Org Admin Emails
            # Update Frequency	Latest of Modifieds
            row = {'URL': url, 'Title': title, 'Organisation': org_title, 'Maintainer': maintainer_name,
                   'Maintainer Email': maintainer_email, 'Org Admins': orgadmin_names,
                   'Org Admin Emails': orgadmin_emails, 'Dataset Start Date': start_date, 'Dataset End Date': end_date,
                   'Update Frequency': update_freq, 'Latest of Modifieds': latest_of_modifieds}
            datasets_flat.append(row)
        emails = dict()
        for id in sorted(all_users_to_email.keys()):
            user = self.users[id]
            basemsg = startmsg % self.get_user_name(user)
            dict_of_lists_add(emails, 'plain', basemsg)
            dict_of_lists_add(emails, 'html', self.email.convert_newlines(basemsg))
            msg = [basemsg]
            htmlmsg = [starthtmlmsg % self.get_user_name(user)]
            for dataset_string, dataset_html_string in all_users_to_email[id]:
                msg.append(dataset_string)
                htmlmsg.append(dataset_html_string)
                dict_of_lists_add(emails, 'plain', dataset_string)
                dict_of_lists_add(emails, 'html', dataset_html_string)
            if sendto is None:
                users_to_email = [user]
            else:
                users_to_email = sendto
            self.email.close_send(users_to_email, 'Check date of dataset for your datasets on HDX', msg, htmlmsg)
        self.email.send_sysadmin_summary(sysadmins, emails, 'All date of dataset emails')
        return datasets_flat

    def process_datasets_dataset_date(self, sendto=None, sysadmins=None):
        logger.info('\n\n*** Checking for datasets where date of dataset has not been updated ***')
        datasets = self.send_datasets_dataset_date_email(sendto=sendto, sysadmins=sysadmins)
        self.sheet.update('DateofDatasets', datasets)

    def send_datasets_datagrid_email(self, sendto=None, sysadmins=None):
        datasets_flat = list()
        datasets = self.databasequeries.get_datasets_datagrid()
        if len(datasets) == 0:
            logger.info('No dataset candidates for the data grid found.')
            return datasets_flat
        startmsg = 'Dear system administrator,\n\nThe new dataset(s) listed below are candidates for the data grid:\n\n'
        msg = [startmsg]
        htmlmsg = [Email.html_start(Email.convert_newlines(startmsg))]
        for dataset in datasets:
            maintainer, orgadmins, _ = self.get_maintainer_orgadmins(dataset)
            dataset_string, dataset_html_string = self.create_dataset_string(dataset, maintainer, orgadmins,
                                                                             sysadmin=True)
            msg.append(dataset_string)
            htmlmsg.append(dataset_html_string)
            url = self.get_dataset_url(dataset)
            title = dataset['title']
            org_title = dataset['organization_title']
            if maintainer:
                maintainer_name, maintainer_email = maintainer
            else:
                maintainer_name, maintainer_email = '', ''
            orgadmin_names = ','.join([x[0] for x in orgadmins])
            orgadmin_emails = ','.join([x[1] for x in orgadmins])
            update_freq = self.get_update_frequency(dataset)
            latest_of_modifieds = dataset['latest_of_modifieds'].isoformat()
            # URL	Title	Organisation	Maintainer	Maintainer Email	Org Admins	Org Admin Emails
            # Update Frequency	Latest of Modifieds
            row = {'URL': url, 'Title': title, 'Organisation': org_title,
                   'Maintainer': maintainer_name, 'Maintainer Email': maintainer_email,
                   'Org Admins': orgadmin_names, 'Org Admin Emails': orgadmin_emails,
                   'Update Frequency': update_freq, 'Latest of Modifieds': latest_of_modifieds}
            datasets_flat.append(row)
        self.email.close_send(self.sysadmins_to_email, 'Candidates for the datagrid', msg, htmlmsg)
        return datasets_flat

    def process_datasets_datagrid(self, sendto=None, sysadmins=None):
        logger.info('\n\n*** Checking for datasets that are candidates for the datagrid ***')
        datasets = self.send_datasets_datagrid_email(sendto=sendto, sysadmins=sysadmins)
        self.sheet.update('Datagrid', datasets)
