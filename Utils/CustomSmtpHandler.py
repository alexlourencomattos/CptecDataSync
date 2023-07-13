from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
from logging import Formatter
import logging
import smtplib
import ssl


css_format = '''
<style>
    #stacktrace_title {text-align: center;}
    #stacktrace_text {font-weight: normal; white-space: pre;}
    table {font-family: arial, sans-serif; border-collapse: collapse; width: 100%;}
    table {font-family: arial, sans-serif; border-collapse: collapse; width: 100%;}
    td, th {border: 1px solid #dddddd; text-align: left; padding: 8px;}
    td:nth-child(1) {font-weight: bold;}
    h2 {text-align: center; font-family: arial, sans-serif;}
</style>
'''


class SmtpHandler(logging.Handler):

    __host: str
    __port: int
    __to_addrs: list
    __subject: str
    __user: str
    __password: str
    __subject_lvl: bool

    def __init__(self, host: str, port: int, user: str, password: str, to_addrs: list, subject, subject_lvl: bool):
        super().__init__()

        self.__host = host
        self.__port = port
        self.__user = user
        self.__password = password
        self.__to_addrs = to_addrs
        self.__subject = subject
        self.__subject_lvl = subject_lvl
        self.setFormatter(Formatter(' %(message)s'))

    def emit(self, record):
        context = ssl.create_default_context()

        message = MIMEMultipart("alternative")
        message["Subject"] = self.__subject
        if self.__subject_lvl:
            message["Subject"] = self.__subject + f' - {logging.getLevelName(self.level)}'
        else:
            message["Subject"] = self.__subject

        message["From"] = self.__user
        message["To"] = ';'.join(self.__to_addrs)
        html_stacktrace = self.format(record).replace('\n', '<br>')
        message_model = f'''
        <!DOCTYPE html>
        <html>
            <head>
                {css_format}
            </head>
            <body>
                <h2>Logger Information</h2>
                <table>
                    <tr>
                        <td>Timestamp</td>
                        <td>{datetime.fromtimestamp(record.created)}</td>
                    </tr>
                    <tr>
                        <td>Module</td>
                        <td>{record.module}</td>
                    </tr>
                    <tr>
                        <td>Filepath</td>
                        <td>{record.pathname}</td>
                    </tr>
                    <tr>
                        <td>Level</td>
                        <td>{record.levelname}</td>
                    </tr>
                    <tr>
                        <td>Message</td>
                        <td>{record.msg}</td>
                    </tr>
                     <tr>
                        <td colspan="2" id="stacktrace_title">Stacktrace</td>
                    </tr>
                    <tr>
                        <td colspan="2" id="stacktrace_text">{html_stacktrace}</td>
                    </tr>
                </table>
            </body>
        </html>'''

        message.attach(MIMEText(message_model, "html"))

        with smtplib.SMTP(self.__host, self.__port) as server:
            server.starttls(context=context)
            server.login(self.__user, self.__password)
            server.sendmail(from_addr=self.__user,
                            to_addrs=self.__to_addrs,
                            msg=message.as_string())
