# -*- coding: utf-8 -*-
"""
Data Freshness Status
---------------------

Determines freshness status
"""


import datetime
import logging

from hdx.freshness.emailer.freshnessemail import Email

logger = logging.getLogger(__name__)


class DataFreshnessStatus:
    object_output_limit = 2

    def __init__(self, datasethelper, databasequeries, email, sheet):
        ''''''
        self.datasethelper = datasethelper
        self.databasequeries = databasequeries
        self.email = email
        self.sheet = sheet

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
                send_to = self.datasethelper.sysadmins_to_email
                stop = False
        else:
            logger.info('No issues with number of datasets.')
            return False
        self.email.htmlify_send(send_to, title, msg)
        return stop

    def process_broken(self, sendto=None):
        logger.info('\n\n*** Checking for broken datasets ***')
        datasets = self.databasequeries.get_broken()
        if len(datasets) == 0:
            logger.info('No broken datasets found.')
            return
        startmsg = 'Dear system administrator,\n\nThe following datasets have broken resources:\n\n'
        msg = [startmsg]
        htmlmsg = [Email.html_start(Email.convert_newlines(startmsg))]

        def create_broken_dataset_string(ds, ma, oa):
            dataset_string, dataset_html_string = \
                self.datasethelper.create_dataset_string(ds, ma, oa, sysadmin=True, include_org=False,
                                                         include_freshness=True)
            Email.output_tabs(msg, htmlmsg, 2)
            msg.append(dataset_string)
            htmlmsg.append(dataset_html_string)
            newline = False
            for i, resource in enumerate(sorted(ds['resources'], key=lambda d: d['name'])):
                if i == self.object_output_limit:
                    Email.output_tabs(msg, htmlmsg, 3)
                if i >= self.object_output_limit:
                    newline = True
                    Email.output_tabs(msg, htmlmsg, 1)
                    msg.append('%s (%s)' % (resource['name'], resource['id']))
                    htmlmsg.append('%s (%s)' % (resource['name'], resource['id']))
                    continue
                resource_string = 'Resource %s (%s) has error: %s!' % \
                                  (resource['name'], resource['id'], resource['error'])
                Email.output_tabs(msg, htmlmsg, 4)
                msg.append('%s\n' % resource_string)
                htmlmsg.append('%s<br>' % resource_string)
            if newline:
                Email.output_newline(msg, htmlmsg)

        def create_cut_down_broken_dataset_string(i, ds):
            if i == self.object_output_limit:
                Email.output_tabs(msg, htmlmsg, 1)
            if i >= self.object_output_limit:
                url = self.datasethelper.get_dataset_url(ds)
                Email.output_tabs(msg, htmlmsg, 1)
                msg.append('%s (%s)' % (ds['title'], url))
                htmlmsg.append('<a href="%s">%s</a>' % (url, ds['title']))
                return True
            return False

        datasets_flat = list()
        for error_type in sorted(datasets):
            Email.output_error(msg, htmlmsg, error_type)
            datasets_error = datasets[error_type]
            for org_title in sorted(datasets_error):
                Email.output_org(msg, htmlmsg, org_title)
                org = datasets_error[org_title]
                newline = False
                for i, dataset_name in enumerate(sorted(org)):
                    dataset = org[dataset_name]
                    maintainer, orgadmins, _ = self.datasethelper.get_maintainer_orgadmins(dataset)
                    cut_down = create_cut_down_broken_dataset_string(i, dataset)
                    if cut_down:
                        newline = True
                    else:
                        create_broken_dataset_string(dataset, maintainer, orgadmins)
                    row = self.sheet.construct_row(self.datasethelper, dataset, maintainer, orgadmins)
                    row['Freshness'] = self.datasethelper.freshness_status.get(dataset['fresh'], 'None')
                    error = list()
                    for resource in sorted(dataset['resources'], key=lambda d: d['name']):
                        error.append('%s:%s' % (resource['name'], resource['error']))
                    row['Error Type'] = error_type
                    row['Error'] = '\n'.join(error)
                    datasets_flat.append(row)
                if newline:
                    Email.output_newline(msg, htmlmsg)
            Email.output_newline(msg, htmlmsg)

        if sendto is None:
            users_to_email = self.datasethelper.sysadmins_to_email
        else:
            users_to_email = sendto
        self.email.close_send(users_to_email, 'Broken datasets', msg, htmlmsg)
        self.sheet.update('Broken', datasets_flat)

    def process_delinquent(self):
        logger.info('\n\n*** Checking for delinquent datasets ***')
        nodatasetsmsg = 'No delinquent datasets found.'
        startmsg = 'Dear system administrator,\n\nThe following datasets have just become delinquent:\n\n'
        subject = 'Delinquent datasets'
        sheetname = 'Delinquent'
        datasets = self.databasequeries.get_status(3)
        self.email.email_admins(self.datasethelper, datasets, nodatasetsmsg, startmsg, subject, self.sheet, sheetname)

    def process_overdue(self, sendto=None, sysadmins=None):
        logger.info('\n\n*** Checking for overdue datasets ***')
        datasets = self.databasequeries.get_status(2)
        nodatasetsmsg = 'No overdue datasets found.'
        startmsg = 'Dear %s,\n\nThe dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). Log into the HDX platform now to update each dataset.\n\n'
        endmsg = '\nTip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.\n'
        subject = 'Time to update your datasets on HDX'
        summary_subject = 'All overdue dataset emails'
        sheetname = None
        self.email.email_users_send_summary(self.datasethelper, False, datasets, nodatasetsmsg, startmsg, endmsg,
                                            sendto,
                                            subject, sysadmins, summary_subject, self.sheet, sheetname)

    def send_maintainer_email(self, datasets):
        nodatasetsmsg = 'No invalid maintainers found.'
        startmsg = 'Dear system administrator,\n\nThe following datasets have an invalid maintainer:\n\n'
        subject = 'Datasets with invalid maintainer'
        sheetname = 'Maintainer'
        self.email.email_admins(self.datasethelper, datasets, nodatasetsmsg, startmsg, subject, self.sheet, sheetname)

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
            url = self.datasethelper.get_organization_url(organization)
            title = organization['title']
            error = organization['error']
            msg.append('%s (%s)' % (title, url))
            htmlmsg.append('<a href="%s">%s</a>' % (url, title))
            msg.append(' with error: %s\n' % error)
            htmlmsg.append(' with error: %s<br>' % error)
            # URL	Title	Problem
            row = {'URL': url, 'Title': title, 'Error': error}
            organizations_flat.append(row)
        self.email.close_send(self.datasethelper.sysadmins_to_email, 'Organizations with invalid admins', msg, htmlmsg)
        self.sheet.update('OrgAdmins', organizations_flat)

    def process_maintainer_orgadmins(self):
        logger.info('\n\n*** Checking for invalid maintainers and organisation administrators ***')
        invalid_maintainers, invalid_orgadmins = \
            self.databasequeries.get_invalid_maintainer_orgadmins(self.datasethelper.organizations,
                                                                  self.datasethelper.users,
                                                                  self.datasethelper.sysadmins)
        self.send_maintainer_email(invalid_maintainers)
        self.send_orgadmins_email(invalid_orgadmins)

    def process_datasets_noresources(self):
        logger.info('\n\n*** Checking for datasets with no resources ***')
        nodatasetsmsg = 'No datasets with no resources found.'
        startmsg = 'Dear system administrator,\n\nThe following datasets have no resources:\n\n'
        subject = 'Datasets with no resources'
        sheetname = 'NoResources'
        datasets = self.databasequeries.get_datasets_noresources()
        self.email.email_admins(self.datasethelper, datasets, nodatasetsmsg, startmsg, subject, self.sheet, sheetname)

    def process_datasets_dataset_date(self, sendto=None, sysadmins=None):
        logger.info('\n\n*** Checking for datasets where date of dataset has not been updated ***')
        datasets = self.databasequeries.get_datasets_dataset_date()
        nodatasetsmsg = 'No datasets with date of dataset needing update found.'
        startmsg = 'Dear %s,\n\nThe dataset(s) listed below have a date of dataset that has not been updated for a while. Log into the HDX platform now to check and if necessary update each dataset.\n\n'
        endmsg = ''
        subject = 'Check date of dataset for your datasets on HDX'
        summary_subject = 'All date of dataset emails'
        sheetname = 'DateofDatasets'
        self.email.email_users_send_summary(self.datasethelper, True, datasets, nodatasetsmsg, startmsg, endmsg, sendto,
                                            subject, sysadmins, summary_subject, self.sheet, sheetname)

    def process_datasets_datagrid(self):
        logger.info('\n\n*** Checking for datasets that are candidates for the datagrid ***')
        nodatasetsmsg = 'No dataset candidates for the data grid found.'
        startmsg = 'Dear system administrator,\n\nThe new dataset(s) listed below are candidates for the data grid:\n\n'
        subject = 'Candidates for the datagrid'
        sheetname = 'Datagrid'
        datasets = self.databasequeries.get_datasets_datagrid()
        self.email.email_admins(self.datasethelper, datasets, nodatasetsmsg, startmsg, subject, self.sheet, sheetname)
