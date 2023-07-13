from Utils.CustomSmtpHandler import SmtpHandler
import logging.handlers
from os import environ
import logging


def get_logger(module_name: str) -> logging.Logger:
    logger = logging.getLogger(module_name)
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)
    # try:
    #     eh = SmtpHandler(
    #         host='smtp.office365.com',
    #         port=587,
    #         user=environ['LOGGER_SMTP_USER'],
    #         password=environ['LOGGER_SMTP_PASSWORD'],
    #         to_addrs=environ['LOGGER_SMTP_DESTINATION'].split(';'),
    #         subject=environ['LOGGER_SMTP_SUBJECT'],
    #         subject_lvl=True)
    #     eh.setLevel(logging.ERROR)
    #     logger.addHandler(eh)
    # except Exception as err:
    #     pass

    logger.addHandler(ch)

    return logger