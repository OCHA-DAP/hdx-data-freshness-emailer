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
from hdx.freshness.database.dbresource import DBResource
from hdx.hdx_configuration import Configuration
from pygsheets import WorksheetNotFound
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker, aliased
from sqlalchemy.pool import NullPool
from sqlalchemy.sql.elements import and_

from hdx.freshness.database.dbdataset import DBDataset
from hdx.freshness.database.dbinfodataset import DBInfoDataset
from hdx.freshness.database.dborganization import DBOrganization
from hdx.freshness.database.dbrun import DBRun
from hdx.freshness.database.base import Base

logger = logging.getLogger(__name__)


class DataFreshnessStatus:
    freshness_status = {0: 'Fresh', 1: 'Due', 2: 'Overdue', 3: 'Delinquent'}
    object_output_limit = 2
    other_error_msg = 'Server Error (may be temporary)'

    def __init__(self, db_url='sqlite:///freshness.db', users=None, ignore_sysadmin_emails=None, now=None):
        ''''''
        engine = create_engine(db_url, poolclass=NullPool, echo=False)
        Session = sessionmaker(bind=engine)
        Base.metadata.create_all(engine)
        self.session = Session()
        if users is None:  # pragma: no cover
            users = User.get_all_users()
        if ignore_sysadmin_emails is None:  # pragma: no cover
            ignore_sysadmin_emails = Configuration.read()['ignore_sysadmin_emails']
        self.sysadmins = list()
        self.users = dict()
        for user in users:
            self.users[user['id']] = user
            if user['sysadmin'] and user['fullname']:
                if user['email'] not in ignore_sysadmin_emails:
                    self.sysadmins.append(user)
        self.orgadmins = dict()
        if now is None:
            self.now = datetime.datetime.utcnow()
        else:
            self.now = now

    def get_cur_prev_runs(self):
        return self.session.query(DBRun.run_number, DBRun.run_date).distinct().order_by(DBRun.run_number.desc()).limit(2).all()

    def check_number_datasets(self, run_numbers, send_failures=None, userclass=User):
        run_date = run_numbers[0][1]
        if self.now < run_date:
            title = 'FAILURE: Future run date!'
            msg = 'Dear system administrator,\n\nIt is highly probable that data freshness has failed!\n'
            send_to = send_failures
        elif self.now - run_date > datetime.timedelta(days=1):
            title = 'FAILURE: No run today!'
            msg = 'Dear system administrator,\n\nIt is highly probable that data freshness has failed!\n'
            send_to = send_failures
        else:
            datasets_today = self.session.query(DBDataset.id).filter(DBDataset.run_number == run_numbers[0][0]).count()
            datasets_previous = self.session.query(DBDataset.id).filter(DBDataset.run_number == run_numbers[1][0]).count()
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
                send_to = self.sysadmins
        htmlmsg = self.html_start(self.htmlify(msg))
        output, htmloutput = self.msg_close(msg, htmlmsg)
        userclass.email_users(send_to, title, output, html_body=htmloutput)
        logger.info(output)

    def get_broken(self, run_numbers):
        datasets = dict()
        columns = [DBResource.id.label('resource_id'), DBResource.name.label('resource_name'),
                   DBResource.dataset_id.label('id'), DBResource.error, DBInfoDataset.name, DBInfoDataset.title,
                   DBInfoDataset.maintainer, DBOrganization.id.label('organization_id'),
                   DBOrganization.title.label('organization_title'), DBDataset.update_frequency, DBDataset.last_modified,
                   DBDataset.what_updated, DBDataset.fresh]
        filters = [DBResource.dataset_id == DBInfoDataset.id, DBInfoDataset.organization_id == DBOrganization.id,
                   DBResource.dataset_id == DBDataset.id, DBDataset.run_number == run_numbers[0][0],
                   DBResource.run_number == DBDataset.run_number, DBResource.error != None,
                   func.date(DBResource.when_checked) == func.date(run_numbers[0][1])]
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

    def get_status(self, run_numbers, status):
        datasets = list()
        no_runs = len(run_numbers)
        if no_runs == 0:
            return datasets
        columns = [DBInfoDataset.id, DBInfoDataset.name, DBInfoDataset.title, DBInfoDataset.maintainer,
                   DBOrganization.id.label('organization_id'), DBOrganization.title.label('organization_title'),
                   DBDataset.update_frequency, DBDataset.last_modified, DBDataset.what_updated]
        filters = [DBDataset.id == DBInfoDataset.id, DBInfoDataset.organization_id == DBOrganization.id,
                   DBDataset.fresh == status, DBDataset.run_number == run_numbers[0][0]]
        if no_runs >= 2:
            # select * from dbdatasets a, dbdatasets b where a.id = b.id and a.fresh = status and a.run_number = 1 and
            # b.fresh = status - 1 and b.run_number = 0;
            DBDataset2 = aliased(DBDataset)
            columns.append(DBDataset2.what_updated.label('prev_what_updated'))
            filters.extend([DBDataset.id == DBDataset2.id, DBDataset2.fresh == status - 1,
                            DBDataset2.run_number == run_numbers[1][0]])
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

    def get_maintainer(self, dataset):
        maintainer = dataset['maintainer']
        return self.users.get(maintainer)

    def get_org_admins(self, dataset):
        organization_id = dataset['organization_id']
        orgadmins = self.orgadmins.get(organization_id)
        if orgadmins is None:
            organization = Organization.read_from_hdx(organization_id)
            orgadmins = organization.get_users(capacity='admin')
            self.orgadmins[organization_id] = orgadmins
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

    @staticmethod
    def get_dataset_url(site_url, dataset):
        return '%sdataset/%s' % (site_url, dataset['name'])

    @staticmethod
    def output_newline(msg, htmlmsg):
        msg.append('\n')
        htmlmsg.append('<br>')

    def create_dataset_string(self, site_url, dataset, maintainer, orgadmins, sysadmin=False, include_org=True, include_freshness=False):
        url = self.get_dataset_url(site_url, dataset)
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

    def send_broken_email(self, site_url, run_numbers, userclass=User, sendto=None):
        datasets_flat = list()
        datasets = self.get_broken(run_numbers)
        if len(datasets) == 0:
            return datasets_flat
        startmsg = 'Dear system administrator,\n\nThe following datasets have broken resources:\n\n'
        msg = [startmsg]
        htmlmsg = [self.html_start(self.htmlify(startmsg))]

        def output_tabs(n=1):
            for i in range(n):
                msg.append('  ')
                htmlmsg.append('&nbsp&nbsp')

        def create_broken_dataset_string(url, ds, ma, oa):
            dataset_string, dataset_html_string = \
                self.create_dataset_string(url, ds, ma, oa, sysadmin=True, include_org=False, include_freshness=True)
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

        def create_cut_down_broken_dataset_string(i, su, ds):
            if i == self.object_output_limit:
                output_tabs(1)
            if i >= self.object_output_limit:
                url = self.get_dataset_url(su, ds)
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
                    cut_down = create_cut_down_broken_dataset_string(i, site_url, dataset)
                    if cut_down:
                        newline = True
                    else:
                        create_broken_dataset_string(site_url, dataset, maintainer, orgadmins)
                    url = self.get_dataset_url(site_url, dataset)
                    if maintainer:
                        maintainer_name, maintainer_email = maintainer
                    else:
                        maintainer_name, maintainer_email = '', ''
                    title = dataset['title']
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
            users_to_email = self.sysadmins
        else:
            users_to_email = sendto
        userclass.email_users(users_to_email, 'Broken datasets', output, html_body=htmloutput)
        logger.info(output)
        return datasets_flat

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
        for dataset in datasets:
            url = dataset['URL']
            new_row = [dataset.get(key, '') for key in keys]
            try:
                rowno = urls.index(url) + 1
                current_row = current_values[rowno]
                new_row[dateadded_ind] = current_row[dateadded_ind]
                no_times = current_row[no_times_ind]
                new_row[no_times_ind] = int(no_times) + 1
                new_row[assigned_ind] = current_row[assigned_ind]
                new_row[status_ind] = current_row[status_ind]
                current_values[rowno] = new_row
            except ValueError:
                new_row[dateadded_ind] = self.now.isoformat()
                new_row[no_times_ind] = 0
                current_values.append(new_row)
        sheet.update_cells('A1', current_values)

    def process_broken(self, site_url, run_numbers, userclass=User, sendto=None, spreadsheet=None):
        datasets = self.send_broken_email(site_url, run_numbers, userclass=userclass, sendto=sendto)
        if spreadsheet is None:
            return
        # sheet must have been set up!
        sheet = spreadsheet.worksheet_by_title('Broken')
        self.update_sheet(sheet, datasets)

    def send_delinquent_email(self, site_url, run_numbers, userclass=User):
        datasets = self.get_status(run_numbers, 3)
        if len(datasets) == 0:
            return
        startmsg = 'Dear system administrator,\n\nThe following datasets have just become delinquent:\n\n'
        msg = [startmsg]
        htmlmsg = [self.html_start(self.htmlify(startmsg))]
        for dataset in sorted(datasets, key=lambda d: (d['organization_title'], d['name'])):
            maintainer, orgadmins, _ = self.get_maintainer_orgadmins(dataset)
            dataset_string, dataset_html_string = self.create_dataset_string(site_url, dataset, maintainer, orgadmins, sysadmin=True)
            msg.append(dataset_string)
            htmlmsg.append(dataset_html_string)
        output, htmloutput = self.msg_close(msg, htmlmsg)
        userclass.email_users(self.sysadmins, 'Delinquent datasets', output, html_body=htmloutput)
        logger.info(output)

    def send_overdue_emails(self, site_url, run_numbers, userclass=User, sendto=None):
        datasets = self.get_status(run_numbers, 2)
        if len(datasets) == 0:
            return
        startmsg = 'Dear %s,\n\nThe dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). Log into the HDX platform now to update each dataset.\n\n'
        starthtmlmsg = self.html_start(self.htmlify(startmsg))
        all_users_to_email = dict()
        for dataset in sorted(datasets, key=lambda d: (d['organization_title'], d['name'])):
            maintainer, orgadmins, users_to_email = self.get_maintainer_orgadmins(dataset)
            dataset_string, dataset_html_string = self.create_dataset_string(site_url, dataset, maintainer, orgadmins)
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
            userclass.email_users(users_to_email, 'Time to update your datasets on HDX', output, html_body=htmloutput)
            logger.info(output)

    def close(self):
        self.session.close()

