from Infrastructure.GisHelper.Tools import RasterTools
from Infrastructure.GisHelper import Tools, Calculate
from Infrastructure.GisHelper.Parser import Parse
from Utils.Logger import get_logger
from multiprocessing import Pool
from itertools import product
from datetime import datetime
from pathlib import Path
import uuid
import re
import os

"""
Module to process CPTEC grib2 files, convert to raster, crop basin shapes and calculate precipitation
"""

logger = get_logger(__name__)
__all__ = ['generate_raster', 'process_precipitation_by_basin']


def generate_raster(cptec_files: dict, dest_path: str) -> list:
    """
    Generate list of tuples with raster properly treated to insert on database
    """

    gpm_rgx = re.compile(r'MERGE_CPTEC_(\d{4})(\d{2})(\d{2})(\d{2})?', re.IGNORECASE)
    result: list = []

    logger.info('Converting CPTEC grib2 files to WKB')
    dest_path = Path(dest_path, 'rasters')
    dest_path.mkdir(exist_ok=True, parents=True)
    for file_name, file_path in cptec_files.items():
        gpm = gpm_rgx.search(file_name)
        year, month, day, hour = gpm.groups()
        hour = 00 if not hour else hour
        datetime_arg = datetime(year=int(year), month=int(month), day=int(day), hour=int(hour))
        if os.environ['GRANULARITY'] == 'DAILY':
            datetime_arg = datetime_arg.date()
        raster_path = Path(dest_path, f'{str(uuid.uuid4())}.tif')
        raster_file = Parse.grib_to_raster(grib=file_path)
        translated_raster = RasterTools.translate(ras_in=raster_file, ras_out=str(raster_path), bands=[1])
        wkb = Parse.raster_to_bytes(translated_raster)
        result.append((datetime_arg, wkb, translated_raster))
    return result


def get_precipitation_by_basin(parameters: tuple) -> dict:
    """
    Crop a rater with basin shape and calculate the average precipitation value in the area
    :return: dict containing the basin attributes and the respective precipitation value
    """

    # unpack the tuple with raster information
    dest_path, basin_data, precipitation_data = parameters
    ref_date, binary_data, ras_in = precipitation_data

    # Get basin attributes from dict
    shape_path = basin_data['shape']
    macro_basin = basin_data['macro_bacia']
    sub_basin = basin_data['sub_bacia']

    logger.debug('Cropping {} - {}'.format(sub_basin, ref_date))

    # Create folder to write the crop result file
    ras_path = Path(dest_path, 'cropped', macro_basin, sub_basin)
    ras_path.mkdir(parents=True, exist_ok=True)

    # Create the file name of result file
    ras_out = str(Path(ras_path, f"{ref_date.strftime('%Y%m%d%H')}.tif"))

    # Execute crop and calculate the average of point in area
    cropped_raster = Tools.RasterTools.clip(ras_in=ras_in, shp=shape_path, ras_out=ras_out)
    precipitation_value = Calculate.RasterCalculate.mean(cropped_raster, band=1, no_data=-9999.0)

    return {'data': ref_date, 'macro_bacia': macro_basin, 'sub_bacia': sub_basin, 'valor': precipitation_value}


def process_precipitation_by_basin(dest_path: str, rasters: list, basins_shapes: list):
    """
    Calculate the average precipitation value for all basins based on all informed rasters
    :param dest_path: Path where the crop result files will be writen on calculate process
    :param rasters: List of rasters containing the precipitation value
    :param basins_shapes: List of basins shapes
    :return: List of dicts containing the date of precipitation, value and the basin attributes
    """

    logger.info('Starting crop rasters process.')

    processes = int(os.environ.get('PROCESSES', '2'))

    tasks = list(product([dest_path], basins_shapes, rasters))
    with Pool(processes=processes) as pool:
        result = list(pool.imap(get_precipitation_by_basin, tasks))

    logger.info('Crop finished successfully.')

    return result
