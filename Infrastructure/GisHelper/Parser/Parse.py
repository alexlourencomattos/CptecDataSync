import uuid

from osgeo.gdalconst import GA_ReadOnly
from osgeo import gdal, osr, ogr
from osgeo.gdal import Dataset
from typing import Union
import pandas as pd
import numpy as np
import os

"""
Module developed to make easier the conversion process between Geo types and python formats

Bibliography:

    WKB Format: https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry
    GrADS: http://cola.gmu.edu/grads/
    OsGeo Library: https://wiki.osgeo.org/wiki/OSGeo_Python_Library 
    
"""

# Skip excessive gdal warnings
gdal.PushErrorHandler('CPLQuietErrorHandler')


def grib_to_raster(grib: str, ras_out: str = None) -> Union[str, Dataset]:
    """
    Read the Grib format and convert to raster
    :param grib: Path of grib file
    :param ras_out: Output raster file path. If None return gdal.Dataset in memory.
    :return:
    """

    if not isinstance(grib, str):
        mem_file = '/vsimem/{}.tif'.format(str(uuid.uuid4()))
        gdal.FileFromMemBuffer(mem_file, grib.read())
        return mem_file
    else:
        src_ds: gdal.Dataset = gdal.Open(grib, GA_ReadOnly)
        if not ras_out:
            return src_ds
        else:
            driver = gdal.GetDriverByName('GTiff')
            driver.CreateCopy(ras_out, src_ds, 0)
            del src_ds
            return ras_out


def text_to_raster(file: str, file_format: str, ras_out: str = None, **kwargs) -> str:
    """
    Convert a text file to a raster file
    :param file: Text file path
    :param file_format: Text file format, that can be CSV or FWF
    :param ras_out: Raster file output path. If None return a Dataset in memory.
    :param kwargs: Set of parameters to read text file. This parameters must supply
    the functions pd.read_csv or pd.read_fwf, according to file type, so that the
    resulting DataFrame has 3 columns called lat, lon and value, respectively, in
    float format.
    """
    # Reading file to na array

    if file_format.upper() == 'FWF':
        data = pd.read_fwf(filepath_or_buffer=file, **kwargs)
    elif file_format.upper() == 'CSV':
        data = pd.read_csv(filepath_or_buffer=file, **kwargs)
    else:
        raise Exception('Invalid format. Only CSV and FWF are expected')

    if list(data.columns) != ['lat', 'lon', 'value']:
        raise Exception('Error on read text file. The resultant Dataframe must have the columns lon, lat and values, '
                        'respectively')

    if not (data['lat'].dtype.name.startswith('float') and data['lon'].dtype.name.startswith('float')):
        raise Exception('Error on read text file. Lat and Lon columns must contains float format.')

    # Extract DataFrame values to a matrix (numpy)
    data = data.sort_values(by=['lat', 'lon']).values

    lat = np.unique(data[:, 0])
    lon = np.unique(data[:, 1])
    lon_min = lon.min()
    lon_max = lon.max()
    lat_min = lat.min()
    lat_max = lat.max()
    values = data[:, 2]

    values = values.reshape(lat.size, lon.size)

    values = np.rot90(values)

    lon_res = (lon_max - lon_min) / float(lon.size)
    lat_res = (lat_max - lat_min) / float(lat.size)

    if not ras_out:
        name = os.path.splitext(os.path.basename(file))[0]
        dest = '/vsimem/{}.tif'.format(name)
    else:
        dest = ras_out

    geotransform = (lat_min, lat_res, 0, lon_max, 0, -lon_res)
    ds = gdal.GetDriverByName('GTiff').Create(dest, lat.size, lon.size, 1, gdal.GDT_Float32)  # Open the file

    ds.SetGeoTransform(geotransform)  # Specify its coordinates
    srs = osr.SpatialReference()  # Establish its coordinate encoding
    srs.ImportFromEPSG(4326)  # This one specifies WGS84 lat long.

    ds.SetProjection(srs.ExportToWkt())  # Exports the coordinate system
    ds.GetRasterBand(1).WriteArray(values)  # Writes my array to the raster

    ds.FlushCache()

    if ras_out:
        del ds
        return dest
    else:
        return ds


def bytes_to_raster(data: bytes, ras_out: str = None, ):
    """
    Convert bytes (Wkb) to raster
    :param data: Bytes data
    :param ras_out: Raster file output path. If None return a Dataset in memory.
    """

    vsipath = '/vsimem/from_postgis'
    gdal.FileFromMemBuffer(vsipath, bytes(data))
    src_ds: gdal.Dataset = gdal.Open(vsipath)

    if not src_ds.GetProjection():
        srs = osr.SpatialReference()  # Establish its coordinate encoding
        srs.ImportFromEPSG(4326)  # This one specifies WGS84 lat long.
        src_ds.SetProjection(srs.ExportToWkt())  # Exports the coordinate system

    if not ras_out:
        return src_ds
    else:
        driver = gdal.GetDriverByName('GTiff')
        driver.CreateCopy(ras_out, src_ds, 0)

        del src_ds
        return ras_out


def raster_to_bytes(raster: str) -> bytes:
    """
    Convert Raster File to bytes in WKB Format
    :param raster: Raster file path
    """

    f = gdal.VSIFOpenL(raster, 'rb')
    gdal.VSIFSeekL(f, 0, 2)
    size = gdal.VSIFTellL(f)
    gdal.VSIFSeekL(f, 0, 0)
    data = gdal.VSIFReadL(1, size, f)
    gdal.VSIFCloseL(f)
    return data


def bytes_to_geometry(data: bytes) -> ogr.Geometry:
    """
    Convert bytes (WKB Format) to Geometry
    :param data: Bytes (WKB Format)
    :return: Geometry Object
    """
    return ogr.CreateGeometryFromWkb(data)


def geometry_to_file(geo: ogr.Geometry, output_path: str, shape_name: str):
    """
    Generate a shape file (.shp) from a Geometry
    :param geo: Geometry object
    :param output_path: Path where shape file will be write
    :param shape_name: Name of shape file, without extension
    :return: Full path of shape file.
    """
    # Now convert it to a shapefile with OGR
    driver = ogr.GetDriverByName('Esri Shapefile')
    ds = driver.CreateDataSource(os.path.join(output_path, f'{shape_name}.shp'))
    srs = osr.SpatialReference()  # Establish its coordinate encoding
    srs.ImportFromEPSG(4326)
    layer = ds.CreateLayer('', srs, ogr.wkbPolygon)
    defn = layer.GetLayerDefn()
    feat = ogr.Feature(defn)
    feat.SetGeometry(geo)
    layer.CreateFeature(feat)
    # destroy these
    del feat
    del ds
    del layer
    return os.path.join(output_path, f'{shape_name}.shp')
