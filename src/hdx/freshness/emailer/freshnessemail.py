# -*- coding: utf-8 -*-
"""
Email
-----

Utilities to handle creating email messages in raw text and HTML formats
"""
import logging

from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.encoding import base64_to_str

logger = logging.getLogger(__name__)


class Email:
    def __init__(self, now, send_emails=None, sysadmins_to_email=None, configuration=None):
        self.now = now
        self.send_emails = send_emails
        if sysadmins_to_email is None:
            self.sysadmins_to_email = configuration['sysadmins_to_email']
            for i, email in enumerate(self.sysadmins_to_email):
                self.sysadmins_to_email[i] = base64_to_str(email)
        else:
            self.sysadmins_to_email = sysadmins_to_email

    def send(self, recipients, subject, text_body, html_body=None, cc=None, bcc=None):
        if self.send_emails is not None:
            if isinstance(recipients, str):
                recipients = [recipients]
            if isinstance(cc, str):
                cc = [cc]
            if isinstance(bcc, str):
                bcc = [bcc]
            subject = '%s (%s)' % (subject, self.now.strftime('%d/%m/%Y'))
            self.send_emails(recipients, subject, text_body, html_body=html_body, cc=cc, bcc=bcc)
        else:
            logger.warning('Not sending any email!')

    def htmlify_send(self, recipients, subject, msg, cc=None, bcc=None):
        text_body, html_body = Email.htmlify(msg)
        self.send(recipients, subject, text_body, html_body, cc=cc, bcc=bcc)
        logger.info(text_body)

    def close_send(self, recipients, subject, msg, htmlmsg, endmsg='', cc=None, bcc=None, log=True):
        text_body, html_body = Email.msg_close(msg, htmlmsg, endmsg)
        self.send(recipients, subject, text_body, html_body, cc=cc, bcc=bcc)
        if log:
            logger.info(text_body)

    @staticmethod
    def get_addressee(dutyofficer, recipients=None, recipients_in_cc=False):
        if dutyofficer and (recipients is None or recipients_in_cc is True):
            return dutyofficer['name']
        else:
            return 'system administrator'

    @staticmethod
    def fill_addressee(msg, htmlmsg, dutyofficer, recipients, recipients_in_cc=False):
        if '%s' not in msg[0]:
            return
        addressee = Email.get_addressee(dutyofficer, recipients, recipients_in_cc=recipients_in_cc)
        msg[0] = msg[0] % addressee
        htmlmsg[0] = htmlmsg[0] % addressee

    def get_recipients_cc(self, dutyofficer, recipients=None, recipients_in_cc=False):
        if recipients is None:
            if dutyofficer:
                return dutyofficer['email'], self.sysadmins_to_email
            else:
                return self.sysadmins_to_email, None
        else:
            if recipients_in_cc:
                if dutyofficer:
                    return dutyofficer['email'], recipients
                else:
                    raise ValueError('Dutyofficer must be supplied if recipients are in cc!')
            else:
                return recipients, None

    def get_recipients_close_send(self, dutyofficer, recipients, subject, msg, htmlmsg, endmsg='', log=True,
                                  recipients_in_cc=False):
        self.fill_addressee(msg, htmlmsg, dutyofficer, recipients, recipients_in_cc=recipients_in_cc)
        recipients, cc = self.get_recipients_cc(dutyofficer, recipients, recipients_in_cc=recipients_in_cc)
        self.close_send(recipients, subject, msg, htmlmsg, endmsg, cc=cc, log=log)

    def send_admin_summary(self, dutyofficer, recipients, emails, subject, startmsg, log=False, recipients_in_cc=False):
        msg = [startmsg]
        htmlmsg = [Email.html_start(Email.convert_newlines(startmsg))]
        msg.extend(emails['plain'])
        htmlmsg.extend(emails['html'])
        self.get_recipients_close_send(dutyofficer, recipients, subject, msg, htmlmsg, log=log,
                                       recipients_in_cc=recipients_in_cc)

    closure = '\nBest wishes,\nHDX Team'

    @classmethod
    def msg_close(cls, msg, htmlmsg, endmsg=''):
        text_body = '%s%s%s' % (''.join(msg), endmsg, Email.closure)
        html_body = cls.html_end('%s%s%s' % (''.join(htmlmsg), Email.convert_newlines(endmsg),
                                              Email.convert_newlines(Email.closure)))
        return text_body, html_body

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
    def prepare_user_emails(datasethelper, include_datasetdate, datasets, sheet, sheetname):
        all_users_to_email = dict()
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
                                 recipients, subject, summary_subject, summary_startmsg, sheet, sheetname,
                                 sysadmins=None):
        if len(datasets) == 0:
            logger.info(nodatasetsmsg)
            return
        all_users_to_email = self.prepare_user_emails(datasethelper, include_datasetdate, datasets, sheet, sheetname)
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
            if recipients is None:
                users_to_email = [user['email']]
            else:
                users_to_email = recipients
            self.close_send(users_to_email, subject, msg, htmlmsg, endmsg)
        self.send_admin_summary(sheet.dutyofficer, sysadmins, emails, summary_subject, summary_startmsg)

    @staticmethod
    def prepare_admin_emails(datasethelper, datasets, startmsg, sheet, sheetname, dutyofficer=None):
        datasets_flat = list()
        msg = [startmsg]
        htmlmsg = [Email.convert_newlines(startmsg)]
        for dataset in sorted(datasets, key=lambda d: (d['organization_title'], d['name'])):
            maintainer, orgadmins, _ = datasethelper.get_maintainer_orgadmins(dataset)
            dataset_string, dataset_html_string = datasethelper.create_dataset_string(dataset, maintainer, orgadmins,
                                                                                      sysadmin=True)
            msg.append(dataset_string)
            htmlmsg.append(dataset_html_string)
            datasets_flat.append(sheet.construct_row(datasethelper, dataset, maintainer, orgadmins))
        sheet.update(sheetname, datasets_flat, dutyofficer_name=dutyofficer['name'])
        return msg, htmlmsg

    def email_admins(self, datasethelper, datasets, nodatasetsmsg, startmsg, subject, sheet, sheetname, recipients=None,
                     dutyofficer=None, recipients_in_cc=False):
        if len(datasets) == 0:
            logger.info(nodatasetsmsg)
            return
        if not dutyofficer:
            dutyofficer = sheet.dutyofficer
        msg, htmlmsg = self.prepare_admin_emails(datasethelper, datasets, startmsg, sheet, sheetname, dutyofficer)
        htmlmsg[0] = Email.html_start(htmlmsg[0])

        self.get_recipients_close_send(dutyofficer, recipients, subject, msg, htmlmsg,
                                       recipients_in_cc=recipients_in_cc)
