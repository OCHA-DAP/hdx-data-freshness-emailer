# -*- coding: utf-8 -*-
'''
Data freshness status:
----------------------

Reads the HDX data freshness database and finds datasets whose status has
changed from overdue to delinquent or from due to overdue.

'''
import logging

from hdx.data.dataset import Dataset
from hdx.data.organization import Organization
from hdx.data.user import User
from hdx.hdx_configuration import Configuration
from sqlalchemy import create_engine
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
    def __init__(self, db_url='sqlite:///freshness.db', users=None, ignore_sysadmin_emails=None):
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

    def get_runs(self):
        return self.session.query(DBRun.run_number).distinct().order_by(DBRun.run_number.desc()).limit(2).all()

    def check_number_datasets(self, run_numbers, send_failures=None, userclass=User):
        datasets_today = self.session.query(DBDataset.id).filter(DBDataset.run_number == run_numbers[0][0]).count()
        datasets_previous = self.session.query(DBDataset.id).filter(DBDataset.run_number == run_numbers[1][0]).count()
        diff_datasets = datasets_previous - datasets_today
        percentage_diff = diff_datasets / datasets_previous
        if percentage_diff > 0.02:
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
        results = self.session.query(*columns).filter(and_(*filters)).all()
        for result in results:
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

    @staticmethod
    def get_user_name(user):
        user_name = user.get('display_name')
        if not user_name:
            user_name = user['fullname']
            if not user_name:
                user_name = user['name']
        return user_name

    def create_dataset_string(self, site_url, dataset, sysadmin=False):
        users_to_email = list()
        url = '%sdataset/%s' % (site_url, dataset['name'])
        msg = list()
        htmlmsg = list()
        msg.append('%s (%s)' % (dataset['title'], url))
        htmlmsg.append('<a href="%s">%s</a>' % (url, dataset['title']))
        if sysadmin:
            orgmsg = ' from %s' % dataset['organization_title']
            msg.append(orgmsg)
            htmlmsg.append(orgmsg)
        maintainer = self.get_maintainer(dataset)
        if maintainer is not None:
            if sysadmin:
                maintainer_name = self.get_user_name(maintainer)
                msg.append(' maintained by %s (%s)' % (maintainer_name, maintainer['email']))
                htmlmsg.append(' maintained by <a href="mailto:%s">%s</a>' % (maintainer['email'], maintainer_name))
            users_to_email.append(maintainer)
        else:
            if sysadmin:
                missing_maintainer = ' with missing maintainer and organization administrators '
                msg.append(missing_maintainer)
                htmlmsg.append(missing_maintainer)

            usermsg = list()
            userhtmlmsg = list()
            for orgadmin in self.get_org_admins(dataset):
                username = self.get_user_name(orgadmin)
                usermsg.append('%s (%s)' % (username, orgadmin['email']))
                userhtmlmsg.append('<a href="mailto:%s">%s</a>' % (orgadmin['email'], username))
                users_to_email.append(orgadmin)
            if sysadmin:
                msg.append(', '.join(usermsg))
                htmlmsg.append(', '.join(userhtmlmsg))
        update_frequency = Dataset.transform_update_frequency('%d' % dataset['update_frequency']).lower()
        msg.append(' with expected update frequency: %s\n' % update_frequency)
        htmlmsg.append(' with expected update frequency: %s<br>' % update_frequency)
        return ''.join(msg), ''.join(htmlmsg), users_to_email

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

    def send_delinquent_email(self, site_url, run_numbers, userclass=User):
        startmsg = 'Dear system administrator,\n\nThe following datasets have just become delinquent:\n\n'
        msg = [startmsg]
        htmlmsg = [self.html_start(self.htmlify(startmsg))]
        datasets = self.get_status(run_numbers, 3)
        if len(datasets) == 0:
            return
        for dataset in sorted(datasets, key=lambda d: (d['organization_title'], d['name'])):
            dataset_string, dataset_html_string, _ = self.create_dataset_string(site_url, dataset, sysadmin=True)
            msg.append(dataset_string)
            htmlmsg.append(dataset_html_string)
        output, htmloutput = self.msg_close(msg, htmlmsg)
        userclass.email_users(self.sysadmins, 'Delinquent datasets', output, html_body=htmloutput)
        logger.info(output)

    def send_overdue_emails(self, site_url, run_numbers, userclass=User, sendto=None):
        startmsg = 'Dear %s,\n\nThe dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). Log into the HDX platform now to update each dataset.\n\n'
        starthtmlmsg = self.html_start(self.htmlify(startmsg))
        datasets = self.get_status(run_numbers, 2)
        if len(datasets) == 0:
            return
        all_users_to_email = dict()
        for dataset in sorted(datasets, key=lambda d: (d['organization_title'], d['name'])):
            dataset_string, dataset_html_string, users_to_email = self.create_dataset_string(site_url, dataset)
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
