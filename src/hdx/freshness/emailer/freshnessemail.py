# -*- coding: utf-8 -*-
"""
Email
-----

Utilities to handle creating email messages in raw text and HTML formats
"""
import logging

from hdx.data.user import User

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

    def close_send(self, send_to, title, msg, htmlmsg, endmsg=''):
        output, htmloutput = Email.msg_close(msg, htmlmsg, endmsg)
        self.send(send_to, title, output, htmloutput)
        logger.info(output)

    closure = '\nBest wishes,\nHDX Team'

    @staticmethod
    def msg_close(msg, htmlmsg, endmsg=''):
        output = '%s%s%s' % (''.join(msg), endmsg, Email.closure)
        htmloutput = Email.html_end('%s%s%s' % (''.join(htmlmsg), Email.convert_newlines(endmsg),
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
    def output_newline(msg, htmlmsg):
        msg.append('\n')
        htmlmsg.append('<br>')

    @staticmethod
    def htmlify(msg):
        htmlmsg = Email.html_start(Email.convert_newlines(msg))
        return Email.msg_close(msg, htmlmsg)
