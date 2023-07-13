from Utils.Logger import get_logger
from ftplib import FTP, error_perm
from datetime import datetime
from io import BytesIO
import pytz
import os

logger = get_logger('FtpClient')


class FtpClient(object):

    __user: str
    __password: str
    __host: str = os.environ['FTP_HOST']
    __ftp: FTP

    def __init__(self, user: str = '', password: str = '', **kwargs):

        self.__user = os.environ['FTP_USER'] if 'FTP_USER' in os.environ else user
        self.__password = os.environ['FTP_PASSWORD'] if 'FTP_PASSWORD' in os.environ else password

        if 'FTP_HOST' in os.environ or 'FTP_HOST' in kwargs:
            self.__host = os.environ['FTP_HOST'] if 'FTP_HOST' in os.environ else kwargs['FTP_HOST']

        self.__ftp = FTP(host=self.__host)

    def __enter__(self):
        self.__ftp.login(user=self.__user, passwd=self.__password)
        return self

    def get_files(self, ftp_path: str, files: list, destination_path: str, not_exists_ok=True) -> dict:

        try:
            # Set current directory to file directory
            self.__ftp.cwd(dirname=ftp_path)
        except error_perm as err:
            if not not_exists_ok:
                raise err
            else:
                return {}

        result = {}
        for file in files:
            try:
                destination_file_path = os.path.join(destination_path, file)
                with open(destination_file_path, 'wb') as fp:
                    self.__ftp.retrbinary('RETR %s' % file, fp.write, blocksize=16000)
                result.update({file: destination_file_path})
            except error_perm as err:
                if not not_exists_ok:
                    raise err
            except Exception as err:
                raise err
        return result

    def get_file_in_memory(self, ftp_path: str, files: list, not_exists_ok=True) -> dict:
        try:
            # Set current directory to file directory
            self.__ftp.cwd(dirname=ftp_path)
        except error_perm as err:
            if not not_exists_ok:
                raise err
            else:
                return {}

        result = {}
        for file in files:
            try:
                content = BytesIO()
                self.__ftp.retrbinary('RETR %s' % file, content.write, blocksize=16000)
                content.seek(0)
                result.update({file: content})
            except error_perm as err:
                if not not_exists_ok:
                    raise err
            except Exception as err:
                raise err
        return result

    def list_files(self, ftp_path):
        try:

            # Set current directory to file directory
            self.__ftp.cwd(dirname=ftp_path)

            files = [f for f in self.__ftp.mlsd()]

            result: list = []

            for file in files:
                result.append({
                    'name': file[0],
                    'size': file[1]['size'],
                    'modify': datetime.strptime(file[1]['modify'], '%Y%m%d%H%M%S.%f').replace(tzinfo=pytz.UTC),
                    'type': file[1]['type']
                })

            return result
        except error_perm as err:
            if err.args[0] == '550 No such directory.':
                logger.error(msg="Informed FTP path {} doesn't exists".format(ftp_path))
            else:
                logger.error(msg="Unhandled error occurs. Error description: {}".format(err))
            return []

    def __exit__(self, exception_type, exception_value, traceback):
        self.__ftp.close()
