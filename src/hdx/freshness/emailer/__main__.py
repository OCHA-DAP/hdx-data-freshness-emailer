#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
REGISTER:
---------

Caller script. Designed to call all other functions.

'''
import argparse
from datetime import datetime, timedelta
import logging
import os
import time
from urllib.parse import urlparse

import psycopg2
import pygsheets
from hdx.data.user import User
from hdx.hdx_configuration import Configuration
from hdx.utilities.dictandlist import key_value_convert
from hdx.utilities.downloader import Download
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.path import script_dir_plus_file
from oauth2client.client import Credentials

from hdx.freshness.emailer.datafreshnessstatus import DataFreshnessStatus

setup_logging()
logger = logging.getLogger(__name__)


def get_date(datestr):
    return datetime.strptime(datestr, '%Y-%m-%d')


def main(hdx_key, user_agent, preprefix, hdx_site, db_url, email_server, gsheet_auth):
    project_config_yaml = script_dir_plus_file('project_configuration.yml', main)
    site_url = Configuration.create(hdx_key=hdx_key, hdx_site=hdx_site,
                                    user_agent=user_agent, preprefix=preprefix,
                                    project_config_yaml=project_config_yaml)
    configuration = Configuration.read()
    logger.info('--------------------------------------------------')
    logger.info('> HDX Site: %s' % site_url)
    email_config = email_server.split(',')
    email_config_dict = {'connection_type': email_config[0], 'host': email_config[1], 'port': int(email_config[2]),
                         'username': email_config[3], 'password': email_config[4]}
    configuration.setup_emailer(email_config_dict=email_config_dict)
    logger.info('--------------------------------------------------')
    logger.info('> Email host: %s' % email_config[1])
    if db_url:
        logger.info('> DB URL: %s' % db_url)
        if 'postgres' in db_url:
            result = urlparse(db_url)
            username = result.username
            password = result.password
            database = result.path[1:]
            hostname = result.hostname
            connecting_string = 'Checking for PostgreSQL...'
            while True:
                try:
                    logger.info(connecting_string)
                    connection = psycopg2.connect(
                        database=database,
                        user=username,
                        password=password,
                        host=hostname,
                        connect_timeout=3
                    )
                    connection.close()
                    logger.info('PostgreSQL is running!')
                    break
                except psycopg2.OperationalError:
                    time.sleep(1)
    else:
        db_url = 'sqlite:///freshness.db'
    with Download() as downloader:
        dutyroster = downloader.download_tabular_cols_as_dicts(configuration['duty_roster_url'], headers=2)
        dutyofficers = key_value_convert(dutyroster['Duty Officer'], keyfn=get_date)

        freshness = DataFreshnessStatus(site_url=site_url, db_url=db_url)  #, send_emails=False)

        logger.info('> GSheet Credentials: %s' % gsheet_auth)
        gc = pygsheets.authorize(credentials=Credentials.new_from_json(gsheet_auth))
        freshness.spreadsheet = gc.open_by_url(configuration['issues_spreadsheet_url'])

        closest_week = next(x for x in sorted(dutyofficers.keys(), reverse=True) if x <= freshness.now)
        freshness.dutyofficer = dutyofficers[closest_week]

        # Send failure messages to Serban and Mike only
        mikeuser = User({'email': 'mcarans@yahoo.co.uk', 'name': 'mcarans', 'sysadmin': True, 'fullname': 'Michael Rans', 'display_name': 'Michael Rans'})
        serbanuser = User({'email': 'teodorescu.serban@gmail.com', 'name': 'serban', 'sysadmin': True, 'fullname': 'Serban Teodorescu', 'display_name': 'Serban Teodorescu'})
        freshness.check_number_datasets(send_failures=[mikeuser, serbanuser])
        freshness.process_broken()
        # temporarily send just to me
        # freshness.process_broken(sendto=[mikeuser])

        freshness.process_delinquent()

        # temporarily send just to me
        # freshness.process_overdue(sendto=[mikeuser])
        freshness.process_overdue()

        freshness.close()
        logger.info('Freshness emailer completed!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Data Freshness Emailer')
    parser.add_argument('-hk', '--hdx_key', default=None, help='HDX api key')
    parser.add_argument('-ua', '--user_agent', default=None, help='user agent')
    parser.add_argument('-pp', '--preprefix', default=None, help='preprefix')
    parser.add_argument('-hs', '--hdx_site', default=None, help='HDX site to use')
    parser.add_argument('-db', '--db_url', default=None, help='Database connection string')
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
    main(hdx_key, user_agent, preprefix, hdx_site, db_url, email_server, gsheet_auth)
