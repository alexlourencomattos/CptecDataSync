from osgeo.gdal import Dataset
from typing import Union, List
from osgeo import gdal
import numpy as np
import subprocess
import shlex
import string
import os

"""
This module use some GDAL Libraries not available on Python API and for this reason make necessary call it through 
the terminal, using python subprocess library.
"""

gdal.PushErrorHandler('CPLQuietErrorHandler')


def mean(ras_in: Union[str, Dataset], band: int, no_data: float) -> float:
    """
    Calculate the arithmetic mean of raster (sum of pixels/point values divided by the number of elements.)
    :param ras_in: Input raster. Can be a raster file path or raster Dataset in memory
    :param band: raster band where the calculus will be executed
    :param no_data: NoData value to be ignored on calculate
    :return: Float with mean value
    """

    if isinstance(ras_in, str):
        ras_in = gdal.Open(ras_in)
    data = ras_in.GetRasterBand(band).ReadAsArray().astype('float')
    # calculate mean without value discarding NoData value
    b_mean: float = np.mean(data[data != no_data])
    return round(b_mean, 2)


def sum(rasters: List[str], ras_out: str, band: int = 1) -> str:
    """
    Realize the sum of rasters in list
    :param rasters: List of raster path to be added
    :param ras_out: Resultant raster
    :param band: band used to calculate
    """
    calc_expression = f'({"+".join(string.ascii_uppercase[idx] for idx in range(len(rasters)))})'
    return calc(rasters, ras_out, calc_expression, [band]*len(rasters))


def rasters_avg(rasters: list, ras_out: str, band: int = 1) -> str:
    """
    Calculate the average of informed raster list
    :param rasters: list of rasters
    :param ras_out: Output raster path
    :param band: band used to calculate
    :return: Path of output raster
    """

    calc_expression = f'({"+".join(string.ascii_uppercase[idx] for idx in range(len(rasters)))})/{len(rasters)}'

    calc(rasters=rasters,
         ras_out=ras_out,
         calc_expression=calc_expression,
         rasters_bands=[band]*len(rasters))

    return ras_out


def calc(rasters: list, ras_out: str, calc_expression: str, rasters_bands: list = None) -> str:
    """
    Execute a calculation, expressed by the expression, between rasters on the list
    :param rasters: List of rasters path to execute the calculation
    :param ras_out: Resultant raster path
    :param calc_expression: Calculation expression. Ex: A+B will execute the sum between rasters.
    :param rasters_bands: List of bands to be processed on calculation. With None band 1 will be used as default
    """
    args = [os.environ.get('GDAL_CALC_PATH', 'gdal_calc.py')]

    rasters_arg = ' '.join(f'-{string.ascii_uppercase[idx]} "{{}}"' for idx, raster in enumerate(rasters))
    args += rasters

    cmd = '{} ' + rasters_arg

    if rasters_bands:
        bands_arg = ' '.join(f'--{string.ascii_uppercase[idx]}_band={{}}' for idx, raster in enumerate(rasters))
        cmd += ' ' + bands_arg
        args += rasters_bands

    # Command to call gdal_calc module
    cmd += ' --outfile="{}" --calc="{}" --overwrite'

    args += [ras_out, calc_expression]

    # build string com  mand
    command = cmd.format(*args)

    # Execute the module call
    command = shlex.split(command) if os.name == 'nt' else [command]
    process = subprocess.run(command, stdout=subprocess.PIPE, shell=True)

    # Read output
    process_ouput = process.stdout.decode()

    # Verify output to get process status
    if '100 - Done' not in process_ouput:
        raise Exception(process_ouput)

    return ras_out
