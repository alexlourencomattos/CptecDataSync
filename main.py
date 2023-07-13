from Modules.Extraction.Exceptions import EmptyDatabaseException, ExtractionException
from Modules import Extraction, Transformation, Load
from Modules.Load.Exceptions import LoadException
from tempfile import TemporaryDirectory
from Utils.Logger import get_logger
from datetime import datetime as dt
from Utils import PowerBiRefresher
from os import environ

logger = get_logger(__name__)


def run():

    try:
        # Reading parameters from env
        sync_days = environ.get('SYNC_DAYS', None)
        sync_from = environ.get('SYNC_FROM', None)
        sync_to = environ.get('SYNC_TO', None)
        granularity = environ.get('GRANULARITY', None)

        # validating parameters
        assert granularity, 'GRANULARITY value must be informed'
        assert granularity.upper() in ['HOURLY', 'DAILY'], 'Only HOURLY and DAILY are allowed for GRANULARITY parameter'
        assert bool(sync_from) == bool(sync_to), 'SYNC-FROM and SYNC-TO must be informed to set period of sync.'
        assert not (bool(sync_from) and bool(sync_days)), 'Only one kind of parameter must be used to informed period.'

        # Converting values
        sync_days = int(sync_days) if sync_days else None

        # Converting date according to granularity
        convert_format = '%Y-%m-%dh%H' if granularity == 'HOURLY' else '%Y-%m-%d'
        sync_from = dt.strptime(sync_from, convert_format) if sync_from else None
        sync_to = dt.strptime(sync_to, convert_format) if sync_to else None

        logger.info('Starting ETL process.')

        # Create a tmp folder to keep process files
        with TemporaryDirectory() as tmp_folder:
            cptec_files = Extraction.get_files(granularity, sync_from, sync_to, sync_days, tmp_folder)
            if cptec_files:
                logger.info(f'{len(cptec_files)} file(s) downloaded from CPTEC FTP Server.')
                basins_shapes = Extraction.get_shapes(dest_path=tmp_folder)
                rasters_files = Transformation.generate_raster(cptec_files, tmp_folder)
                if not environ.get('SKIP_WRITE_RASTER', 'FALSE') == 'TRUE':
                    Load.write_raster([r[:2] for r in rasters_files])
                if not environ.get('SKIP_WRITE_BASIN', 'FALSE') == 'TRUE':
                    basin_data = Transformation.process_precipitation_by_basin(tmp_folder, rasters_files, basins_shapes)
                    Load.write_basin_data(basin_data)
                if environ.get('POWERBI_REFRESH', 'FALSE') == 'TRUE':
                    PowerBiRefresher.refresh()

            else:
                logger.info('No files available on period or they already on database')

    except AssertionError as err:
        logger.error(err)
    except (ExtractionException, EmptyDatabaseException, LoadException) as err:
        logger.error(err)
    except Exception as err:
        logger.exception(f'Unhandled exception. Details: {err}')
    finally:
        logger.info('CPTEC GPM Merge ETL process finished.')


if __name__ == '__main__':
    run()
