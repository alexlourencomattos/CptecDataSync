import uuid
from rasterio.enums import Resampling
from typing import Union
from osgeo import gdal
from osgeo.gdal import Dataset, GA_Update
import rasterio

# Skip GDAL excessive warnings
gdal.PushErrorHandler('CPLQuietErrorHandler')


def clip(ras_in: Union[str, Dataset], shp: str, ras_out: str = None) -> Union[str, Dataset]:
    """
    Apply a crop in the raster using a shape file.
    :param ras_in: Input raster to be cropped
    :param shp: Input shape used to crop
    :param ras_out: Result raster output path. If None return a Dataset in memory.
    :return: Raster Raster file path
    """
    kwargs = {'cropToCutline': True,
              'cutlineDSName': shp,
              'dstNodata': -9999,
              'dstSRS': 'epsg:4326'}

    # kwargs = {'cropToCutline': True,
    #           'cutlineDSName': shp,
    #           'dstNodata': -9999}
    # gdal.WarpOptions(dstSRS='EPSG:4326')

    if not ras_out:
        dest = '/vsimem/{}.tif'.format(str(uuid.uuid4()))
    else:
        dest = ras_out

    ds: Dataset = gdal.Warp(destNameOrDestDS=dest, srcDSOrSrcDSTab=ras_in, **kwargs)

    if ras_out:
        del ds
        return ras_out
    else:
        return ds


def upscale(ras_in: str, ras_out: str, scale: int):
    """
    Apply the upscale to a raster based on scale factor
    :param ras_in: Input raster file path
    :param ras_out: Output raster file path
    :param scale: Scale factor
    """

    with rasterio.open(ras_in) as raster:

        # rescale the metadata
        height = int(raster.height * scale)
        width = int(raster.width * scale)

        data = raster.read(
                out_shape=(raster.count, height, width),
                resampling=Resampling.nearest,
            )

        ras_transform = raster.transform * raster.transform.scale(
            (raster.width / data.shape[-1]),
            (raster.height / data.shape[-2])
        )

        profile = raster.profile
        profile.update(transform=ras_transform, driver='GTiff', height=height, width=width)

        with rasterio.open(ras_out, 'w', **profile) as dataset:
            dataset.write(data)
            return ras_out


def transform(ras_in: str, ras_transform: tuple):
    """
    Takes a dataset, and changes its original geotransform to an arbitrary geotransform
    """
    ds = gdal.Open(ras_in, GA_Update)

    if ds is None:
        return

    ds.SetGeoTransform(ras_transform)
    del ds


def translate(ras_in: Union[str, Dataset], bands: list,  ras_out: str = None) -> Union[str, Dataset]:

    if isinstance(ras_in, str):
        ds = gdal.Open(ras_in)
    else:
        ds = ras_in

    if not ras_out:
        dest = '/vsimem/{}.tif'.format(str(uuid.uuid4()))
    else:
        dest = ras_out

    ds = gdal.Translate(dest, ds, bandList=bands)

    if ras_out:
        del ds

    return dest