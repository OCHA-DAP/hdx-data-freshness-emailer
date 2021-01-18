# -*- coding: utf-8 -*-
"""
DatasetHelper
-------------

Dataset helper functions
"""
from datetime import datetime

from hdx.data.dataset import Dataset
from hdx.data.organization import Organization
from hdx.data.user import User
from hdx.utilities.dictandlist import dict_of_lists_add

from hdx.freshness.emailer.freshnessemail import Email


class DatasetHelper:
    freshness_status = {0: 'Fresh', 1: 'Due', 2: 'Overdue', 3: 'Delinquent'}

    def __init__(self, site_url, users=None, organizations=None):
        self.site_url = site_url
        if users is None:  # pragma: no cover
            users = User.get_all_users()
        self.users = dict()
        self.sysadmins = dict()
        for user in users:
            userid = user['id']
            self.users[userid] = user
            if user['sysadmin']:
                self.sysadmins[userid] = user

        self.organizations = dict()
        if organizations is None:  # pragma: no cover
            organizations = Organization.get_all_organization_names(all_fields=True, include_users=True)
        for organization in organizations:
            users_per_capacity = dict()
            for user in organization['users']:
                dict_of_lists_add(users_per_capacity, user['capacity'], user['id'])
            self.organizations[organization['id']] = users_per_capacity

    @staticmethod
    def get_dataset_dates(dataset):
        dataset_date = dataset['dataset_date']
        if not dataset_date:
            return None, None
        if '-' in dataset_date:
            date_start, date_end = dataset_date.split('-')
        else:
            date_start = date_end = dataset_date
        return datetime.strptime(date_start, '%m/%d/%Y'), datetime.strptime(date_end, '%m/%d/%Y')

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
        return '%s/dataset/%s' % (self.site_url, dataset['name'])

    def get_organization_url(self, organization):
        return '%s/organization/%s' % (self.site_url, organization['name'])

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
