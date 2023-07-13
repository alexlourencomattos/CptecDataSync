from datetime import timedelta as delta, datetime
from pathlib import Path
from Modules.Extraction.Exceptions import *
from Modules.Extraction import DataSource
from Infrastructure.Ftp import FtpClient
from Utils.Logger import get_logger
from itertools import groupby
from typing import Any
import os

logger = get_logger('Modules.CPTEC.Cptec')
__all__ = ['get_files']


def get_files(granularity: str, sync_from: Any, sync_to: Any, sync_days: Any, dest_path: str) -> dict:

    period = calculate_period(granularity=granularity, sync_from=sync_from, sync_to=sync_to, sync_days=sync_days)
    files_paths = generate_files_paths(granularity=granularity, period=period)
    files = download_data(files_paths=files_paths, dest_path=dest_path)
    return files


def calculate_period(granularity: str, sync_from: datetime, sync_to: datetime, sync_days: int) -> dict:
    """
    Function to calculate all units on download period according to range and granularity
    """
    if sync_to and sync_from:
        period_boundary = (sync_from, sync_to)
    elif sync_days:
        end_period = datetime.today().replace(minute=0, second=0, microsecond=0)
        period_boundary = (end_period - delta(days=sync_days), end_period)
    else:
        last_sync = DataSource.get_last_sync()
        if not last_sync:
            raise EmptyDatabaseException('No previous data on the destine table. The firs sync must have a period '
                                         'manually determined.')

        last_sync += delta(hours=1) if os.environ['GRANULARITY'] == 'HOURLY' else delta(days=1)
        end_period = last_sync if granularity == 'DAILY' else datetime.now().replace(minute=0, second=0, microsecond=0)
        period_boundary = (last_sync, end_period)

    period_range = int((period_boundary[1] - period_boundary[0]).total_seconds() / 3600)
    period_range = period_range if granularity == 'HOURLY' else int(period_range / 24)

    logger.info('Retrieving GPM {} files from {} to {}'.format(granularity, period_boundary[0], period_boundary[1]))

    # Calculate all units in period and group units according to granularity.
    if granularity == 'DAILY':
        period_units = [period_boundary[1] - delta(days=i) for i in range(period_range + 1)]
        group = groupby(period_units, lambda x: (x.year, x.month))
        search_group = {k: [d.date() for d in list(g)] for k, g in group}
    else:
        period_units = [period_boundary[1] - delta(hours=i) for i in range(period_range + 1)]
        group = groupby(period_units, lambda x: (x.year, x.month, x.day))
        search_group = {k: [d for d in list(g)] for k, g in group}

    return search_group


def generate_files_paths(granularity: str, period: dict) -> dict:
    """
    Generate a dict containing the path struct of files based on period to be downloaded and granularity
    :param granularity: Granularity of data (DAILY or HOURLY)
    :param period: Dict with period grouped
    """

    base_dir = os.environ['CPTEC_FTP_DIR']
    path_struct = {}
    for k, g in period.items():
        for file in g:
            if granularity == 'HOURLY':
                year, month, day = k
                ftp_path = f'{base_dir}/HOURLY/{year}/{month:02}/{day:02}'
                file_name = f"MERGE_CPTEC_{file.strftime('%Y%m%d%H')}.grib2"
            else:
                year, month = k
                ftp_path = f'{base_dir}/DAILY/{year}/{month:02}'
                file_name = f"MERGE_CPTEC_{file.strftime('%Y%m%d')}.grib2"

            path_files = path_struct.get(ftp_path, []) + [file_name]
            path_struct.update({ftp_path: path_files})

    return path_struct


def download_data(files_paths: dict, dest_path: str) -> dict:
    """
    Download files from CPTEC FTP Server.
    :param dest_path:
    :param files_paths: Dict struct where keys are the paths and values list of files names
    :return: Dict where keys are files names and the values the a ByteIO object containing the content
    """

    downloaded_files = {}
    download_path = Path(dest_path, 'cptec_files')
    download_path.mkdir(exist_ok=True)

    with FtpClient() as ftp:
        for path, files in files_paths.items():
            ftp_files = ftp.get_files(ftp_path=path,
                                      files=files,
                                      destination_path=str(download_path),
                                      not_exists_ok=True)
            downloaded_files = {**downloaded_files, **ftp_files}

    return downloaded_files
