# -*- coding: utf-8 -*-
"""
Database queries
----------------

Queries to the freshness database
"""

import datetime
import logging
import re
from collections import OrderedDict

from hdx.freshness.database.dbdataset import DBDataset
from hdx.freshness.database.dbinfodataset import DBInfoDataset
from hdx.freshness.database.dborganization import DBOrganization
from hdx.freshness.database.dbresource import DBResource
from hdx.freshness.database.dbrun import DBRun
from sqlalchemy.orm import aliased
from sqlalchemy.sql.elements import and_

from hdx.freshness.emailer.datasethelper import DatasetHelper

logger = logging.getLogger(__name__)


class DatabaseQueries:
    other_error_msg = 'Server Error (may be temporary)'

    def __init__(self, session, now):
        ''''''
        self.session = session
        self.run_numbers = self.get_cur_prev_runs(now)
        if len(self.run_numbers) < 2:
            logger.warning('Less than 2 runs!')

    def get_run_numbers(self):
        return self.run_numbers

    def get_cur_prev_runs(self, now):
        all_run_numbers = self.session.query(DBRun.run_number, DBRun.run_date).distinct().order_by(
            DBRun.run_number.desc()).all()
        last_ind = len(all_run_numbers) - 1
        for i, run_number in enumerate(all_run_numbers):
            if run_number[1] < now:
                if i == last_ind:
                    return [run_number]
                else:
                    return [run_number, all_run_numbers[i + 1]]
        return list()

    def get_number_datasets(self):
        datasets_today = self.session.query(DBDataset.id).filter(
            DBDataset.run_number == self.run_numbers[0][0]).count()
        datasets_previous = self.session.query(DBDataset.id).filter(
            DBDataset.run_number == self.run_numbers[1][0]).count()
        return datasets_today, datasets_previous

    def get_broken(self):
        datasets = dict()
        if len(self.run_numbers) == 0:
            return datasets
        columns = [DBResource.id.label('resource_id'), DBResource.name.label('resource_name'),
                   DBResource.dataset_id.label('id'), DBResource.error, DBInfoDataset.name, DBInfoDataset.title,
                   DBInfoDataset.maintainer, DBOrganization.id.label('organization_id'),
                   DBOrganization.title.label('organization_title'), DBDataset.update_frequency,
                   DBDataset.latest_of_modifieds,
                   DBDataset.what_updated, DBDataset.fresh]
        filters = [DBResource.dataset_id == DBInfoDataset.id, DBInfoDataset.organization_id == DBOrganization.id,
                   DBResource.dataset_id == DBDataset.id, DBDataset.run_number == self.run_numbers[0][0],
                   DBResource.run_number == DBDataset.run_number, DBResource.error != None,
                   DBResource.when_checked > self.run_numbers[1][1]]
        query = self.session.query(*columns).filter(and_(*filters))
        norows = 0
        for norows, result in enumerate(query):
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

        logger.info('SQL query returned %d rows.' % norows)
        return datasets

    def get_status(self, status):
        datasets = list()
        no_runs = len(self.run_numbers)
        if no_runs == 0:
            return datasets
        columns = [DBInfoDataset.id, DBInfoDataset.name, DBInfoDataset.title, DBInfoDataset.maintainer,
                   DBOrganization.id.label('organization_id'), DBOrganization.title.label('organization_title'),
                   DBDataset.update_frequency, DBDataset.latest_of_modifieds, DBDataset.what_updated]
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
        norows = 0
        for norows, result in enumerate(query):
            dataset = dict()
            for i, column in enumerate(columns):
                dataset[column.key] = result[i]
            if dataset['what_updated'] == 'nothing':
                dataset['what_updated'] = dataset['prev_what_updated']
            del dataset['prev_what_updated']
            datasets.append(dataset)

        logger.info('SQL query returned %d rows.' % norows)
        return datasets

    def get_invalid_maintainer_orgadmins(self, organizations, users, sysadmins):
        invalid_maintainers = list()
        invalid_orgadmins = dict()
        no_runs = len(self.run_numbers)
        if no_runs == 0:
            return invalid_maintainers, invalid_orgadmins
        columns = [DBInfoDataset.id, DBInfoDataset.name, DBInfoDataset.title, DBInfoDataset.maintainer,
                   DBOrganization.id.label('organization_id'), DBOrganization.name.label('organization_name'),
                   DBOrganization.title.label('organization_title'),
                   DBDataset.update_frequency, DBDataset.latest_of_modifieds, DBDataset.what_updated]
        filters = [DBDataset.id == DBInfoDataset.id, DBInfoDataset.organization_id == DBOrganization.id,
                   DBDataset.run_number == self.run_numbers[0][0]]
        query = self.session.query(*columns).filter(and_(*filters))
        norows = 0
        for norows, result in enumerate(query):
            dataset = dict()
            for i, column in enumerate(columns):
                dataset[column.key] = result[i]
            maintainer_id = dataset['maintainer']
            organization_id = dataset['organization_id']
            organization_name = dataset['organization_name']
            organization = organizations[organization_id]
            admins = organization.get('admin')

            def get_orginfo(error):
                return {'id': organization_id, 'name': organization_name,
                        'title': dataset['organization_title'],
                        'error': error}

            if admins:
                all_sysadmins = True
                nonexistantids = list()
                for adminid in admins:
                    admin = users.get(adminid)
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
            if maintainer_id in sysadmins:
                continue
            invalid_maintainers.append(dataset)

        logger.info('SQL query returned %d rows.' % norows)
        return invalid_maintainers, invalid_orgadmins

    def get_datasets_noresources(self):
        datasets_noresources = list()
        no_runs = len(self.run_numbers)
        if no_runs == 0:
            return datasets_noresources
        columns = [DBInfoDataset.id, DBInfoDataset.name, DBInfoDataset.title, DBInfoDataset.maintainer,
                   DBOrganization.id.label('organization_id'), DBOrganization.name.label('organization_name'),
                   DBOrganization.title.label('organization_title'),
                   DBDataset.update_frequency, DBDataset.latest_of_modifieds, DBDataset.what_updated]
        filters = [DBDataset.id == DBInfoDataset.id, DBInfoDataset.organization_id == DBOrganization.id,
                   DBDataset.run_number == self.run_numbers[0][0], DBDataset.what_updated == 'no resources']
        query = self.session.query(*columns).filter(and_(*filters))
        norows = 0
        for norows, result in enumerate(query):
            dataset = dict()
            for i, column in enumerate(columns):
                dataset[column.key] = result[i]
            datasets_noresources.append(dataset)

        logger.info('SQL query returned %d rows.' % norows)
        return datasets_noresources

    def get_datasets_modified_yesterday(self):
        datasets = OrderedDict()
        no_runs = len(self.run_numbers)
        if no_runs < 2:
            return datasets
        columns = [DBInfoDataset.id, DBInfoDataset.name, DBInfoDataset.title, DBInfoDataset.maintainer,
                   DBOrganization.id.label('organization_id'), DBOrganization.name.label('organization_name'),
                   DBOrganization.title.label('organization_title'), DBDataset.dataset_date,
                   DBDataset.update_frequency, DBDataset.latest_of_modifieds, DBDataset.what_updated]
        filters = [DBDataset.id == DBInfoDataset.id, DBInfoDataset.organization_id == DBOrganization.id,
                   DBDataset.run_number == self.run_numbers[0][0],
                   DBDataset.latest_of_modifieds > self.run_numbers[1][1]]
        query = self.session.query(*columns).filter(and_(*filters))
        norows = 0
        for norows, result in enumerate(query):
            dataset = dict()
            for i, column in enumerate(columns):
                dataset[column.key] = result[i]
            datasets[dataset['id']] = dataset
        logger.info('SQL query returned %d rows.' % norows)
        return datasets

    def get_datasets_dataset_date(self):
        datasets = self.get_datasets_modified_yesterday()
        datasets_dataset_date = list()
        for dataset in datasets.values():
            update_frequency = dataset['update_frequency']
            if update_frequency == 0:
                update_frequency = 1
            if update_frequency == -1 or update_frequency == -2:
                update_frequency = 365
            _, dataset_date = DatasetHelper.get_dataset_dates(dataset)
            if not dataset_date:
                continue
            delta = dataset['latest_of_modifieds'] - dataset_date
            if delta <= datetime.timedelta(days=update_frequency):
                continue
            datasets_dataset_date.append(dataset)
        return datasets_dataset_date
