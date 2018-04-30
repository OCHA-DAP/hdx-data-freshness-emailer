# -*- coding: utf-8 -*-
'''
Data freshness status:
----------------------

Reads the HDX data freshness database and finds datasets whose status has
changed from overdue to delinquent or from due to overdue.

'''
import datetime
import logging
import re

from hdx.data.dataset import Dataset
from hdx.data.organization import Organization
from hdx.data.user import User
from hdx.freshness.database.base import Base
from hdx.freshness.database.dbdataset import DBDataset
from hdx.freshness.database.dbinfodataset import DBInfoDataset
from hdx.freshness.database.dborganization import DBOrganization
from hdx.freshness.database.dbresource import DBResource
from hdx.freshness.database.dbrun import DBRun
from hdx.hdx_configuration import Configuration
from hdx.utilities.dictandlist import dict_of_lists_add
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker, aliased
from sqlalchemy.pool import NullPool
from sqlalchemy.sql.elements import and_

logger = logging.getLogger(__name__)


class DataFreshnessStatus:
    freshness_status = {0: 'Fresh', 1: 'Due', 2: 'Overdue', 3: 'Delinquent'}
    object_output_limit = 2
    other_error_msg = 'Server Error (may be temporary)'

    def __init__(self, site_url, db_url='sqlite:///freshness.db', users=None, organizations=None,
                 ignore_sysadmin_emails=None, now=None, send_emails=True):
        ''''''
        self.site_url = site_url
        engine = create_engine(db_url, poolclass=NullPool, echo=False)
        Session = sessionmaker(bind=engine)
        Base.metadata.create_all(engine)
        self.session = Session()
        if users is None:  # pragma: no cover
            users = User.get_all_users()
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
        if now is None:
            self.now = datetime.datetime.utcnow()
        else:
            self.now = now
        self.run_numbers = self.get_cur_prev_runs()
        self.send_emails = send_emails
        self.spreadsheet = None
        self.dutyofficer = None

    def get_cur_prev_runs(self):
        all_run_numbers = self.session.query(DBRun.run_number, DBRun.run_date).distinct().order_by(DBRun.run_number.desc()).all()
        last_ind = len(all_run_numbers) - 1
        for i, run_number in enumerate(all_run_numbers):
            if run_number[1] < self.now:
                if i == last_ind:
                    return [run_number]
                else:
                    return [run_number, all_run_numbers[i+1]]
        return list()

    def check_number_datasets(self, send_failures=None, userclass=User):
        run_date = self.run_numbers[0][1]
        if self.now < run_date:
            title = 'FAILURE: Future run date!'
            msg = 'Dear system administrator,\n\nIt is highly probable that data freshness has failed!\n'
            send_to = send_failures
        elif self.now - run_date > datetime.timedelta(days=1):
            title = 'FAILURE: No run today!'
            msg = 'Dear system administrator,\n\nIt is highly probable that data freshness has failed!\n'
            send_to = send_failures
        elif len(self.run_numbers) == 2:
            datasets_today = self.session.query(DBDataset.id).filter(DBDataset.run_number == self.run_numbers[0][0]).count()
            datasets_previous = self.session.query(DBDataset.id).filter(DBDataset.run_number == self.run_numbers[1][0]).count()
            diff_datasets = datasets_previous - datasets_today
            percentage_diff = diff_datasets / datasets_previous
            if percentage_diff <= 0.02:
                return
            if percentage_diff == 1.0:
                title = 'FAILURE: No datasets today!'
                msg = 'Dear system administrator,\n\nIt is highly probable that data freshness has failed!\n'
                send_to = send_failures
            else:
                title = 'WARNING: Fall in datasets on HDX today!'
                msg = 'Dear system administrator,\n\nThere are %d (%d%%) fewer datasets today than yesterday on HDX!\n' % \
                         (diff_datasets, percentage_diff * 100)
                send_to = self.sysadmins_to_email
        else:
            return
        htmlmsg = self.html_start(self.htmlify(msg))
        output, htmloutput = self.msg_close(msg, htmlmsg)
        if self.send_emails:
            userclass.email_users(send_to, title, output, html_body=htmloutput)
        logger.info(output)

    def get_broken(self):
        datasets = dict()
        if len(self.run_numbers) == 0:
            return datasets
        columns = [DBResource.id.label('resource_id'), DBResource.name.label('resource_name'),
                   DBResource.dataset_id.label('id'), DBResource.error, DBInfoDataset.name, DBInfoDataset.title,
                   DBInfoDataset.maintainer, DBOrganization.id.label('organization_id'),
                   DBOrganization.title.label('organization_title'), DBDataset.update_frequency, DBDataset.last_modified,
                   DBDataset.what_updated, DBDataset.fresh]
        filters = [DBResource.dataset_id == DBInfoDataset.id, DBInfoDataset.organization_id == DBOrganization.id,
                   DBResource.dataset_id == DBDataset.id, DBDataset.run_number == self.run_numbers[0][0],
                   DBResource.run_number == DBDataset.run_number, DBResource.error != None,
                   func.date(DBResource.when_checked) == func.date(self.run_numbers[0][1])]
        query = self.session.query(*columns).filter(and_(*filters))
        for result in query:
            row = dict()
            for i, column in enumerate(columns):
                row[column.key] = result[i]
            regex = '.Client(.*)Error '
            error = row['error']
            search_exception = re.search(regex, error)
            if search_exception:
                exception_string = search_exception.group(0)[1:-1]
            else:
                exception_string = self.other_error_msg
            datasets_error = datasets.get(exception_string, dict())
            datasets[exception_string] = datasets_error

            org_title = row['organization_title']
            org = datasets_error.get(org_title, dict())
            datasets_error[org_title] = org
            
            dataset_name = row['name']
            dataset = org.get(dataset_name, dict())
            org[dataset_name] = dataset

            resources = dataset.get('resources', list())
            dataset['resources'] = resources

            resource = {'id': row['resource_id'], 'name': row['resource_name'], 'error': error}
            resources.append(resource)
            del row['resource_id']
            del row['resource_name']
            del row['error']
            dataset.update(row)

        return datasets

    def get_status(self, status):
        datasets = list()
        no_runs = len(self.run_numbers)
        if no_runs == 0:
            return datasets
        columns = [DBInfoDataset.id, DBInfoDataset.name, DBInfoDataset.title, DBInfoDataset.maintainer,
                   DBOrganization.id.label('organization_id'), DBOrganization.title.label('organization_title'),
                   DBDataset.update_frequency, DBDataset.last_modified, DBDataset.what_updated]
        filters = [DBDataset.id == DBInfoDataset.id, DBInfoDataset.organization_id == DBOrganization.id,
                   DBDataset.fresh == status, DBDataset.run_number == self.run_numbers[0][0]]
        if no_runs >= 2:
            # select * from dbdatasets a, dbdatasets b where a.id = b.id and a.fresh = status and a.run_number = 1 and
            # b.fresh = status - 1 and b.run_number = 0;
            DBDataset2 = aliased(DBDataset)
            columns.append(DBDataset2.what_updated.label('prev_what_updated'))
            filters.extend([DBDataset.id == DBDataset2.id, DBDataset2.fresh == status - 1,
                            DBDataset2.run_number == self.run_numbers[1][0]])
        query = self.session.query(*columns).filter(and_(*filters))
        for result in query:
            dataset = dict()
            for i, column in enumerate(columns):
                dataset[column.key] = result[i]
            if dataset['what_updated'] == 'nothing':
                dataset['what_updated'] = dataset['prev_what_updated']
            del dataset['prev_what_updated']
            datasets.append(dataset)
        return datasets

    def get_invalid_maintainer_orgadmins(self):
        invalid_maintainers = list()
        invalid_orgadmins = dict()
        no_runs = len(self.run_numbers)
        if no_runs == 0:
            return invalid_maintainers, invalid_orgadmins
        columns = [DBInfoDataset.id, DBInfoDataset.name, DBInfoDataset.title, DBInfoDataset.maintainer,
                   DBOrganization.id.label('organization_id'), DBOrganization.name.label('organization_name'),
                   DBOrganization.title.label('organization_title'),
                   DBDataset.update_frequency, DBDataset.last_modified, DBDataset.what_updated]
        filters = [DBDataset.id == DBInfoDataset.id, DBInfoDataset.organization_id == DBOrganization.id,
                   DBDataset.run_number == self.run_numbers[0][0]]
        query = self.session.query(*columns).filter(and_(*filters))
        for result in query:
            dataset = dict()
            for i, column in enumerate(columns):
                dataset[column.key] = result[i]
            maintainer_id = dataset['maintainer']
            organization_id = dataset['organization_id']
            organization_name = dataset['organization_name']
            organization = self.organizations[organization_id]
            admins = organization.get('admin')

            def get_orginfo(error):
                return {'id': organization_id, 'name': organization_name,
                        'title': dataset['organization_title'],
                        'error': error}

            if admins:
                all_sysadmins = True
                nonexistantids = list()
                for adminid in admins:
                    admin = self.users.get(adminid)
                    if not admin:
                        nonexistantids.append(adminid)
                    else:
                        if admin['sysadmin'] is False:
                            all_sysadmins = False
                if nonexistantids:
                    invalid_orgadmins[organization_name] = \
                        get_orginfo('The following org admins do not exist: %s!' % ', '.join(nonexistantids))
                elif all_sysadmins:
                    invalid_orgadmins[organization_name] = get_orginfo('All org admins are sysadmins!')
                if maintainer_id in admins:
                    continue
            else:
                invalid_orgadmins[organization_name] = get_orginfo('No org admins defined!')
            editors = organization.get('editor', [])
            if maintainer_id in editors:
                continue
            if maintainer_id in self.sysadmins:
                continue
            invalid_maintainers.append(dataset)
        return invalid_maintainers, invalid_orgadmins

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

    @staticmethod
    def output_newline(msg, htmlmsg):
        msg.append('\n')
        htmlmsg.append('<br>')

    def create_dataset_string(self, dataset, maintainer, orgadmins, sysadmin=False, include_org=True, include_freshness=False):
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
        self.output_newline(msg, htmlmsg)

        return ''.join(msg), ''.join(htmlmsg)

    @staticmethod
    def msg_close(msg, htmlmsg, endmsg=''):
        closure = '\nBest wishes,\nHDX Team'
        output = '%s%s%s' % (''.join(msg), endmsg, closure)
        htmloutput = DataFreshnessStatus.html_end('%s%s%s' % (''.join(htmlmsg), DataFreshnessStatus.htmlify(endmsg),
                                                              DataFreshnessStatus.htmlify(closure)))
        return output, htmloutput

    @staticmethod
    def htmlify(msg):
        return msg.replace('\n', '<br>')

    @staticmethod
    def html_start(msg):
        return '''\
<html>
  <head></head>
  <body>
    <span>%s''' % msg

    @staticmethod
    def html_end(msg):
        return '''%s
      <br/><br/>
      <small>
        <p>
          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>
        </p>
        <p>
          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>
        </p>
      </small>
    </span>
  </body>
</html>
''' % msg

    def update_sheet(self, sheet, datasets):
        if sheet is None:
            return
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
        sheet.update_cells('A1', current_values)

    def send_broken_email(self, userclass=User, sendto=None):
        datasets_flat = list()
        datasets = self.get_broken()
        if len(datasets) == 0:
            return datasets_flat
        startmsg = 'Dear system administrator,\n\nThe following datasets have broken resources:\n\n'
        msg = [startmsg]
        htmlmsg = [self.html_start(self.htmlify(startmsg))]

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
                self.output_newline(msg, htmlmsg)

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
            self.output_newline(msg, htmlmsg)

        def output_org(title):
            msg.append(title)
            htmlmsg.append('<b><i>%s</i></b>' % title)
            self.output_newline(msg, htmlmsg)

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
                    last_modified = dataset['last_modified'].isoformat()
                    fresh = self.freshness_status.get(dataset['fresh'], 'None')
                    error = list()
                    for resource in sorted(dataset['resources'], key=lambda d: d['name']):
                        error.append('%s:%s' % (resource['name'], resource['error']))
                    # Date Added    URL	Title	Organisation	Maintainer	Maintainer Email	Org Admins
                    # Org Admin Emails	Update Frequency    Last Modified	Freshness	Error Type	Error
                    row = {'URL': url, 'Title': title, 'Organisation': org_title,
                           'Maintainer': maintainer_name, 'Maintainer Email': maintainer_email,
                           'Org Admins': orgadmin_names, 'Org Admin Emails': orgadmin_emails,
                           'Update Frequency': update_freq, 'Last Modified': last_modified, 'Freshness': fresh,
                           'Error Type': error_type, 'Error': '\n'.join(error)}
                    datasets_flat.append(row)
                if newline:
                    self.output_newline(msg, htmlmsg)
            self.output_newline(msg, htmlmsg)

        output, htmloutput = self.msg_close(msg, htmlmsg)
        if sendto is None:
            users_to_email = self.sysadmins_to_email
        else:
            users_to_email = sendto
        if self.send_emails:
            userclass.email_users(users_to_email, 'Broken datasets', output, html_body=htmloutput)
        logger.info(output)
        return datasets_flat

    def process_broken(self, userclass=User, sendto=None):
        datasets = self.send_broken_email(userclass=userclass, sendto=sendto)
        if self.spreadsheet is None or self.dutyofficer is None:
            return
        # sheet must have been set up!
        sheet = self.spreadsheet.worksheet_by_title('Broken')
        self.update_sheet(sheet, datasets)

    def send_delinquent_email(self, userclass=User):
        datasets_flat = list()
        datasets = self.get_status(3)
        if len(datasets) == 0:
            return datasets_flat
        startmsg = 'Dear system administrator,\n\nThe following datasets have just become delinquent:\n\n'
        msg = [startmsg]
        htmlmsg = [self.html_start(self.htmlify(startmsg))]
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
            last_modified = dataset['last_modified'].isoformat()
            # URL	Title	Organisation	Maintainer	Maintainer Email	Org Admins	Org Admin Emails
            # Update Frequency	Last Modified
            row = {'URL': url, 'Title': title, 'Organisation': org_title,
                   'Maintainer': maintainer_name, 'Maintainer Email': maintainer_email,
                   'Org Admins': orgadmin_names, 'Org Admin Emails': orgadmin_emails,
                   'Update Frequency': update_freq, 'Last Modified': last_modified}
            datasets_flat.append(row)
        output, htmloutput = self.msg_close(msg, htmlmsg)
        if self.send_emails:
            userclass.email_users(self.sysadmins_to_email, 'Delinquent datasets', output, html_body=htmloutput)
        logger.info(output)
        return datasets_flat

    def process_delinquent(self, userclass=User):
        datasets = self.send_delinquent_email(userclass=userclass)
        if self.spreadsheet is None or self.dutyofficer is None:
            return
        # sheet must have been set up!
        sheet = self.spreadsheet.worksheet_by_title('Delinquent')
        self.update_sheet(sheet, datasets)

    def send_overdue_emails(self, userclass=User, sendto=None):
        datasets = self.get_status(2)
        if len(datasets) == 0:
            return
        startmsg = 'Dear %s,\n\nThe dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). Log into the HDX platform now to update each dataset.\n\n'
        starthtmlmsg = self.html_start(self.htmlify(startmsg))
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
        for id in sorted(all_users_to_email.keys()):
            user = self.users[id]
            msg = [startmsg % self.get_user_name(user)]
            htmlmsg = [starthtmlmsg % self.get_user_name(user)]
            for dataset_string, dataset_html_string in all_users_to_email[id]:
                msg.append(dataset_string)
                htmlmsg.append(dataset_html_string)
            endmsg = '\nTip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.\n'
            output, htmloutput = self.msg_close(msg, htmlmsg, endmsg)
            if sendto is None:
                users_to_email = [user]
            else:
                users_to_email = sendto
            if self.send_emails:
                userclass.email_users(users_to_email, 'Time to update your datasets on HDX', output, html_body=htmloutput)
            logger.info(output)

    def process_overdue(self, userclass=User, sendto=None):
        self.send_overdue_emails(userclass=userclass, sendto=sendto)

    def send_maintainer_email(self, invalid_maintainers, userclass=User):
        datasets_flat = list()
        if len(invalid_maintainers) == 0:
            return datasets_flat
        startmsg = 'Dear system administrator,\n\nThe following datasets have an invalid maintainer:\n\n'
        msg = [startmsg]
        htmlmsg = [self.html_start(self.htmlify(startmsg))]
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
            last_modified = dataset['last_modified'].isoformat()
            # URL	Title	Organisation	Maintainer	Maintainer Email	Org Admins	Org Admin Emails
            # Update Frequency	Last Modified
            row = {'URL': url, 'Title': title, 'Organisation': org_title,
                   'Maintainer': maintainer_name, 'Maintainer Email': maintainer_email,
                   'Org Admins': orgadmin_names, 'Org Admin Emails': orgadmin_emails,
                   'Update Frequency': update_freq, 'Last Modified': last_modified}
            datasets_flat.append(row)
        output, htmloutput = self.msg_close(msg, htmlmsg)
        if self.send_emails:
            userclass.email_users(self.sysadmins_to_email, 'Datasets with invalid maintainer', output,
                                  html_body=htmloutput)
        logger.info(output)
        return datasets_flat

    def send_orgadmins_email(self, invalid_orgadmins, userclass=User):
        organizations_flat = list()
        if len(invalid_orgadmins) == 0:
            return organizations_flat
        startmsg = 'Dear system administrator,\n\nThe following organizations have an invalid admin:\n\n'
        msg = [startmsg]
        htmlmsg = [self.html_start(self.htmlify(startmsg))]
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
        output, htmloutput = self.msg_close(msg, htmlmsg)
        if self.send_emails:
            userclass.email_users(self.sysadmins_to_email, 'Organizations with invalid admins', output,
                                  html_body=htmloutput)
        logger.info(output)
        return organizations_flat

    def process_maintainer_orgadmins(self, userclass=User):
        invalid_maintainers, invalid_orgadmins = self.get_invalid_maintainer_orgadmins()
        datasets = self.send_maintainer_email(invalid_maintainers, userclass=userclass)
        if self.spreadsheet is not None and self.dutyofficer is not None:
            # sheet must have been set up!
            sheet = self.spreadsheet.worksheet_by_title('Maintainer')
            self.update_sheet(sheet, datasets)
        datasets = self.send_orgadmins_email(invalid_orgadmins, userclass=userclass)
        if self.spreadsheet is None or self.dutyofficer is None:
            return
        # sheet must have been set up!
        sheet = self.spreadsheet.worksheet_by_title('OrgAdmins')
        self.update_sheet(sheet, datasets)

    def close(self):
        self.session.close()

