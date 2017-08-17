# -*- coding: utf-8 -*-
'''
Data freshness status:
----------------------

Reads the HDX data freshness database and finds datasets whose status has
changed from overdue to delinquent or from due to overdue.

'''
import logging

from freshness.database.base import Base
from freshness.database.dbinfodataset import DBInfoDataset
from freshness.database.dborganization import DBOrganization
from freshness.database.dbrun import DBRun
from freshness.database.dbdataset import DBDataset
from hdx.data.dataset import Dataset
from hdx.data.organization import Organization
from hdx.data.user import User
from hdx.hdx_configuration import Configuration
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, aliased
from sqlalchemy.pool import NullPool
from sqlalchemy.sql.elements import and_

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

    def get_status(self, status):
        datasets = list()
        run_numbers = self.session.query(DBRun.run_number).distinct().order_by(DBRun.run_number.desc()).limit(2).all()
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

    def create_dataset_string(self, site_url, dataset):
        users_to_email = list()
        url = '%sdataset/%s' % (site_url, dataset['name'])
        update_frequency = Dataset.transform_update_frequency('%d' % dataset['update_frequency'])
        msg = list()
        msg.append('%s (%s) from %s ' % (dataset['title'], url, dataset['organization_title']))
        maintainer = self.get_maintainer(dataset)
        if maintainer is not None:
            maintainer_name = self.get_user_name(maintainer)
            msg.append('maintained by %s (%s)' % (maintainer_name, maintainer['email']))
            users_to_email.append(maintainer)
        else:
            msg.append('with missing maintainer and organization administrators ')
            usermsg = list()
            for orgadmin in self.get_org_admins(dataset):
                usermsg.append('%s (%s)' % (self.get_user_name(orgadmin), orgadmin['email']))
                users_to_email.append(orgadmin)
            msg.append(','.join(usermsg))
        msg.append(' with update frequency: %s\n' % update_frequency)
        return ''.join(msg), users_to_email

    def send_delinquent_email(self, site_url, userclass=User):
        msg = ['Dear system administrator,\n\nThe following datasets have just become delinquent:\n\n']
        datasets = self.get_status(3)
        if len(datasets) == 0:
            return
        for dataset in datasets:
            dataset_string, _ = self.create_dataset_string(site_url, dataset)
            msg.append(dataset_string)
        output = ''.join(msg)
        userclass.email_users(self.sysadmins, 'Delinquent datasets', output)
        logger.info(output)

    def send_overdue_emails(self, site_url, userclass=User, sendto=None):
        startmsg = 'Dear %s,\n\nThe following datasets are now overdue for update:\n\n'
        datasets = self.get_status(2)
        if len(datasets) == 0:
            return
        all_users_to_email = dict()
        for dataset in datasets:
            dataset_string, users_to_email = self.create_dataset_string(site_url, dataset)
            for user in users_to_email:
                id = user['id']
                output_list = all_users_to_email.get(id)
                if output_list is None:
                    output_list = list()
                    all_users_to_email[id] = output_list
                output_list.append(dataset_string)
        for id in sorted(all_users_to_email.keys()):
            user = self.users[id]
            msg = [startmsg % self.get_user_name(user)]
            for dataset_string in all_users_to_email[id]:
                msg.append(dataset_string)
            output = ''.join(msg)
            if sendto is None:
                users_to_email = [user]
            else:
                users_to_email = sendto
            userclass.email_users(users_to_email, 'Overdue datasets', output)
            logger.info(output)

    def close(self):
        self.session.close()

