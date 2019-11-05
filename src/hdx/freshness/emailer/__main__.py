#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
REGISTER:
---------

Caller script. Designed to call all other functions.

"""
import argparse
import datetime
import logging
from os import getenv

from hdx.data.user import User
from hdx.facades.keyword_arguments import facade
from hdx.hdx_configuration import Configuration
from hdx.utilities.database import Database
from hdx.utilities.dictandlist import args_to_dict
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.encoding import base64_to_str
from hdx.utilities.path import script_dir_plus_file

from hdx.freshness.emailer.databasequeries import DatabaseQueries
from hdx.freshness.emailer.datafreshnessstatus import DataFreshnessStatus
from hdx.freshness.emailer.datasethelper import DatasetHelper
from hdx.freshness.emailer.freshnessemail import Email
from hdx.freshness.emailer.sheet import Sheet

setup_logging()
logger = logging.getLogger(__name__)


def main(db_url, db_params, email_server, gsheet_auth, **ignore):
    configuration = Configuration.read()
    if email_server:
        email_config = email_server.split(',')
        email_config_dict = {'connection_type': email_config[0], 'host': email_config[1], 'port': int(email_config[2]),
                             'username': email_config[3], 'password': email_config[4]}
        if len(email_config) > 5:
            email_config_dict['sender'] = email_config[5]
        configuration.setup_emailer(email_config_dict=email_config_dict)
        logger.info('> Email host: %s' % email_config[1])
        send_emails = True
    else:
        logger.info('> No email host!')
        send_emails = False
    if db_params:
        params = args_to_dict(db_params)
    elif db_url:
        params = Database.get_params_from_sqlalchemy_url(db_url)
    else:
        params = {'driver': 'sqlite', 'database': 'freshness.db'}
    logger.info('> Database parameters: %s' % params)
    with Database(**params) as session:
        email = Email(send_emails=send_emails)
        now = datetime.datetime.utcnow()
        sheet = Sheet(now)

        failure_list = list()
        for address in configuration['failure_emails']:
            failure_list.append(User({'email': base64_to_str(address)}))
        error = sheet.setup_input(configuration)
        if error:
            email.htmlify_send(failure_list, 'Error reading DP duty roster or data grid curation sheet!',
                               error)
        else:
            error = sheet.setup_output(configuration, gsheet_auth)
            if error:
                email.htmlify_send(failure_list, 'Error accessing datasets with issues Google sheet!', error)
            else:
                datasethelper = DatasetHelper(site_url=configuration.get_hdx_site_url())
                databasequeries = DatabaseQueries(session=session, now=now)
                freshness = DataFreshnessStatus(datasethelper=datasethelper, databasequeries=databasequeries,
                                                email=email, sheet=sheet)
                freshness.process_datasets_datagrid()
                # if not freshness.check_number_datasets(now, send_failures=failure_list):
                #     freshness.process_broken()
                #     # temporarily send just to me
                #     # freshness.process_broken(sendto=[failure_list[0]])
                #
                #     freshness.process_delinquent()
                #
                #     freshness.process_overdue(sysadmins=[failure_list[0]])
                #     # temporarily send just to me
                #     # freshness.process_overdue(sendto=[failure_list[0]])
                #
                #     freshness.process_maintainer_orgadmins()
                #
                #     freshness.process_datasets_noresources()
                #
                #     # freshness.process_datasets_dataset_date(sysadmins=[failure_list[0]])
                #     # temporarily send just to me
                #     freshness.process_datasets_dataset_date(sendto=[failure_list[0]], sysadmins=[failure_list[0]])

    logger.info('Freshness emailer completed!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Data Freshness Emailer')
    parser.add_argument('-hk', '--hdx_key', default=None, help='HDX api key')
    parser.add_argument('-ua', '--user_agent', default=None, help='user agent')
    parser.add_argument('-pp', '--preprefix', default=None, help='preprefix')
    parser.add_argument('-hs', '--hdx_site', default=None, help='HDX site to use')
    parser.add_argument('-db', '--db_url', default=None, help='Database connection string')
    parser.add_argument('-dp', '--db_params', default=None, help='Database connection parameters. Overrides --db_url.')
    parser.add_argument('-es', '--email_server', default=None, help='Email server to use')
    parser.add_argument('-gs', '--gsheet_auth', default=None, help='Credentials for accessing Google Sheets')
    args = parser.parse_args()
    hdx_key = args.hdx_key
    if hdx_key is None:
        hdx_key = getenv('HDX_KEY')
    user_agent = args.user_agent
    if user_agent is None:
        user_agent = getenv('USER_AGENT')
        if user_agent is None:
            user_agent = 'freshness-emailer'
    preprefix = args.preprefix
    if preprefix is None:
        preprefix = getenv('PREPREFIX')
    hdx_site = args.hdx_site
    if hdx_site is None:
        hdx_site = getenv('HDX_SITE', 'prod')
    db_url = args.db_url
    if db_url is None:
        db_url = getenv('DB_URL')
    if db_url and '://' not in db_url:
        db_url = 'postgresql://%s' % db_url
    email_server = args.email_server
    if email_server is None:
        email_server = getenv('EMAIL_SERVER')
    gsheet_auth = args.gsheet_auth
    if gsheet_auth is None:
        gsheet_auth = getenv('GSHEET_AUTH')
    project_config_yaml = script_dir_plus_file('project_configuration.yml', main)
    facade(main, hdx_key=hdx_key, user_agent=user_agent, preprefix=preprefix, hdx_site=hdx_site,
           project_config_yaml=project_config_yaml, db_url=db_url, db_params=args.db_params,
           email_server=email_server, gsheet_auth=gsheet_auth)
