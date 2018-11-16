#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
REGISTER:
---------

Caller script. Designed to call all other functions.

'''
import argparse
import logging
import os
from datetime import datetime

import pygsheets
from hdx.data.user import User
from hdx.hdx_configuration import Configuration
from hdx.utilities.database import Database
from hdx.utilities.dictandlist import key_value_convert, args_to_dict
from hdx.utilities.downloader import Download
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.path import script_dir_plus_file
from oauth2client.client import Credentials

from hdx.freshness.emailer.datafreshnessstatus import DataFreshnessStatus

setup_logging()
logger = logging.getLogger(__name__)


def get_date(datestr):
    return datetime.strptime(datestr, '%Y-%m-%d')


def main(hdx_key, user_agent, preprefix, hdx_site, db_url, db_params, email_server, gsheet_auth):
    project_config_yaml = script_dir_plus_file('project_configuration.yml', main)
    site_url = Configuration.create(hdx_key=hdx_key, hdx_site=hdx_site,
                                    user_agent=user_agent, preprefix=preprefix,
                                    project_config_yaml=project_config_yaml)
    configuration = Configuration.read()
    logger.info('--------------------------------------------------')
    logger.info('> HDX Site: %s' % site_url)
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
        with Download() as downloader:
            dutyroster = downloader.download_tabular_cols_as_dicts(configuration['duty_roster_url'], headers=2)
            dutyofficers = key_value_convert(dutyroster['Duty Officer'], keyfn=get_date)

            freshness = DataFreshnessStatus(site_url=site_url, session=session, send_emails=send_emails)

            if gsheet_auth:
                logger.info('> GSheet Credentials: %s' % gsheet_auth)
                gc = pygsheets.authorize(credentials=Credentials.new_from_json(gsheet_auth))
                freshness.spreadsheet = gc.open_by_url(configuration['issues_spreadsheet_url'])
            else:
                logger.info('> No GSheet Credentials!')
                freshness.spreadsheet = None
            logger.info('--------------------------------------------------')
            closest_week = next(x for x in sorted(dutyofficers.keys(), reverse=True) if x <= freshness.now)
            freshness.dutyofficer = dutyofficers[closest_week]
            logger.info('Duty officer: %s' % freshness.dutyofficer)

            # Send failure messages to Serban and Mike only
            mikeuser = User(
                {'email': 'mcarans@yahoo.co.uk', 'name': 'mcarans', 'sysadmin': True, 'fullname': 'Michael Rans',
                 'display_name': 'Michael Rans'})
            serbanuser = User({'email': 'teodorescu.serban@gmail.com', 'name': 'serban', 'sysadmin': True,
                               'fullname': 'Serban Teodorescu', 'display_name': 'Serban Teodorescu'})
            if not freshness.check_number_datasets(send_failures=[mikeuser, serbanuser]):
                freshness.process_broken()
                # temporarily send just to me
                # freshness.process_broken(sendto=[mikeuser])

                freshness.process_delinquent()

                freshness.process_overdue()
                # temporarily send just to me
                # freshness.process_overdue(sendto=[mikeuser])

                freshness.process_maintainer_orgadmins()

                freshness.process_datasets_noresources()

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
        hdx_key = os.getenv('HDX_KEY')
    user_agent = args.user_agent
    if user_agent is None:
        user_agent = os.getenv('USER_AGENT')
        if user_agent is None:
            user_agent = 'freshness-emailer'
    preprefix = args.preprefix
    if preprefix is None:
        preprefix = os.getenv('PREPREFIX')
    hdx_site = args.hdx_site
    if hdx_site is None:
        hdx_site = os.getenv('HDX_SITE', 'prod')
    db_url = args.db_url
    if db_url is None:
        db_url = os.getenv('DB_URL')
    if db_url and '://' not in db_url:
        db_url = 'postgresql://%s' % db_url
    email_server = args.email_server
    if email_server is None:
        email_server = os.getenv('EMAIL_SERVER')
    gsheet_auth = args.gsheet_auth
    if gsheet_auth is None:
        gsheet_auth = os.getenv('GSHEET_AUTH')
    main(hdx_key, user_agent, preprefix, hdx_site, db_url, args.db_params, email_server, gsheet_auth)
