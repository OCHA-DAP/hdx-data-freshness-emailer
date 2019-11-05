# -*- coding: utf-8 -*-
"""
Email
-----

Utilities to handle creating email messages in raw text and HTML formats
"""
import logging

from hdx.data.user import User
from hdx.utilities.dictandlist import dict_of_lists_add

logger = logging.getLogger(__name__)


class Email:
    def __init__(self, userclass=User, send_emails=True):
        self.userclass = userclass
        self.send_emails = send_emails

    def send(self, send_to, title, output, htmloutput):
        if self.send_emails:
            self.userclass.email_users(send_to, title, output, html_body=htmloutput)
        else:
            logger.warning('Not sending any email!')

    def htmlify_send(self, send_to, title, msg):
        output, htmloutput = Email.htmlify(msg)
        self.send(send_to, title, output, htmloutput)
        logger.info(output)

    def close_send(self, send_to, title, msg, htmlmsg, endmsg='', log=True):
        output, htmloutput = Email.msg_close(msg, htmlmsg, endmsg)
        self.send(send_to, title, output, htmloutput)
        if log:
            logger.info(output)

    def send_sysadmin_summary(self, sysadmins, emails, title):
        if sysadmins:
            startmsg = 'Dear system administrator,\n\n'
            msg = [startmsg]
            htmlmsg = [Email.html_start(Email.convert_newlines(startmsg))]
            msg.extend(emails['plain'])
            htmlmsg.extend(emails['html'])
            self.close_send(sysadmins, title, msg, htmlmsg, log=False)

    closure = '\nBest wishes,\nHDX Team'

    @classmethod
    def msg_close(cls, msg, htmlmsg, endmsg=''):
        output = '%s%s%s' % (''.join(msg), endmsg, Email.closure)
        htmloutput = cls.html_end('%s%s%s' % (''.join(htmlmsg), Email.convert_newlines(endmsg),
                                              Email.convert_newlines(Email.closure)))
        return output, htmloutput

    @staticmethod
    def convert_newlines(msg):
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

    @staticmethod
    def output_tabs(msg, htmlmsg, n=1):
        for i in range(n):
            msg.append('  ')
            htmlmsg.append('&nbsp&nbsp')

    @staticmethod
    def output_newline(msg, htmlmsg):
        msg.append('\n')
        htmlmsg.append('<br>')

    @classmethod
    def htmlify(cls, msg):
        htmlmsg = cls.html_start(Email.convert_newlines(msg))
        return cls.msg_close(msg, htmlmsg)

    @classmethod
    def output_error(cls, msg, htmlmsg, error):
        msg.append(error)
        htmlmsg.append('<b>%s</b>' % error)
        cls.output_newline(msg, htmlmsg)

    @classmethod
    def output_org(cls, msg, htmlmsg, title):
        msg.append(title)
        htmlmsg.append('<b><i>%s</i></b>' % title)
        cls.output_newline(msg, htmlmsg)

    @staticmethod
    def prepare_emails(datasethelper, include_datasetdate, datasets, nodatasetsmsg, sheet, sheetname):
        all_users_to_email = dict()
        if len(datasets) == 0:
            logger.info(nodatasetsmsg)
            return all_users_to_email
        datasets_flat = list()
        for dataset in sorted(datasets, key=lambda d: (d['organization_title'], d['name'])):
            maintainer, orgadmins, users_to_email = datasethelper.get_maintainer_orgadmins(dataset)
            dataset_string, dataset_html_string = datasethelper.create_dataset_string(dataset, maintainer, orgadmins,
                                                                                      include_datasetdate=include_datasetdate)
            for user in users_to_email:
                id = user['id']
                output_list = all_users_to_email.get(id)
                if output_list is None:
                    output_list = list()
                    all_users_to_email[id] = output_list
                output_list.append((dataset_string, dataset_html_string))
            row = sheet.construct_row(datasethelper, dataset, maintainer, orgadmins)
            if include_datasetdate:
                start_date, end_date = datasethelper.get_dataset_dates(dataset)
                row['Dataset Start Date'] = start_date.isoformat()
                row['Dataset End Date'] = end_date.isoformat()
            datasets_flat.append(row)
        if sheetname is not None:
            sheet.update(sheetname, datasets_flat)
        return all_users_to_email

    def email_users_send_summary(self, datasethelper, include_datasetdate, datasets, nodatasetsmsg, startmsg, endmsg,
                                 sendto, subject, sysadmins, summary_subject, sheet, sheetname):
        all_users_to_email = self.prepare_emails(datasethelper, include_datasetdate, datasets, nodatasetsmsg, sheet,
                                                 sheetname)

        starthtmlmsg = self.html_start(self.convert_newlines(startmsg))
        if '$dashboard' in startmsg:
            startmsg = startmsg.replace('$dashboard', 'dashboard')
            starthtmlmsg = starthtmlmsg.replace('$dashboard',
                                                '<a href="https://data.humdata.org/dashboard/datasets">dashboard</a>')
        emails = dict()
        for id in sorted(all_users_to_email.keys()):
            user = datasethelper.users[id]
            username = datasethelper.get_user_name(user)
            basemsg = startmsg % username
            dict_of_lists_add(emails, 'plain', basemsg)
            dict_of_lists_add(emails, 'html', self.convert_newlines(basemsg))
            msg = [basemsg]
            htmlmsg = [starthtmlmsg % username]
            for dataset_string, dataset_html_string in all_users_to_email[id]:
                msg.append(dataset_string)
                htmlmsg.append(dataset_html_string)
                dict_of_lists_add(emails, 'plain', dataset_string)
                dict_of_lists_add(emails, 'html', dataset_html_string)
            if sendto is None:
                users_to_email = [user]
            else:
                users_to_email = sendto
            self.close_send(users_to_email, subject, msg, htmlmsg, endmsg)
        self.send_sysadmin_summary(sysadmins, emails, summary_subject)

    def email_admins(self, datasethelper, datasets, nodatasetsmsg, startmsg, subject, sheet, sheetname, sendto=None,
                     dutyofficer=None):
        datasets_flat = list()
        if len(datasets) == 0:
            logger.info(nodatasetsmsg)
            return
        msg = [startmsg]
        htmlmsg = [Email.html_start(Email.convert_newlines(startmsg))]
        for dataset in sorted(datasets, key=lambda d: (d['organization_title'], d['name'])):
            maintainer, orgadmins, _ = datasethelper.get_maintainer_orgadmins(dataset)
            dataset_string, dataset_html_string = datasethelper.create_dataset_string(dataset, maintainer, orgadmins,
                                                                                      sysadmin=True)
            msg.append(dataset_string)
            htmlmsg.append(dataset_html_string)
            datasets_flat.append(sheet.construct_row(datasethelper, dataset, maintainer, orgadmins))
        if sendto is None:
            users_to_email = datasethelper.sysadmins_to_email
        else:
            users_to_email = sendto
        self.close_send(users_to_email, subject, msg, htmlmsg)
        sheet.update(sheetname, datasets_flat, dutyofficer=dutyofficer)
