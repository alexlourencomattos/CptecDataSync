from Modules.Extraction.Exceptions import ExtractionException
from Infrastructure.Postgres import Postgres
from Infrastructure.GisHelper import Parser
from datetime import datetime, date, time
from Utils.Logger import get_logger
from pathlib import Path
import psycopg2
import os

"""
Module to read from Postgres database basins shapes.
"""


logger = get_logger(__name__)
__all__ = ['get_last_sync', 'get_shapes']


def read_shapes() -> list:
    """
    Retrieve shapes of database
    :return: Return a list of dicts containing the shapes and their attributes
    """
    try:
        model = 'eta' if os.environ['MODEL'] == 'ETA40' else 'gfs'
        logger.info('Retrieving shapes from database.')
        read_statement = f'''select
                                c.codigo_ana,
                                c.sub_bacia,
                                c.bacia,
                                c.centroide,
                                c.contorno_{model} as nome_cotorno,
                                (select 
                                    ST_AsBinary(contorno) 
                                from smap.contorno 
                                where nome_referencia = c.contorno_{model} limit 1) as contorno
                            from smap.config as c
                            where c.contorno_{model} is not null;'''

        database_name = os.environ['SHAPES_DATABASE']

        db = Postgres(database=database_name)

        shapes = db.read(query=read_statement, to_dict=True)

        return shapes

    except KeyError as err:
        raise ExtractionException('{} parameter must be informed'.format(err.args[0]))
    except Exception as err:
        logger.exception(err)
        raise ExtractionException('Error on retrieve shapes from database')


def get_shapes(dest_path: str) -> list:
    """
    Get Shapes from Database and convert to .shp files
    :param dest_path: Path where .shp files will be writen
    :return: List of dictionaries containing sub-basins attributes and .shp path
    """

    logger.info('Retrieving basins shapes from database.')

    # Retrieve shapes from database
    shapes = read_shapes()
    result = []

    logger.info('Generating shp files')
    print(shapes)
    #shapes = shapes.remove({'codigo_ana': 'PSATJIRA', 'sub_bacia': 'Ficticio Jirau', 'bacia': 'Madeira', 'centroide': '(-64.66,-9.26)', 'nome_cotorno': 'NaN', 'contorno': None})
    # iterate each shape
    for shape in shapes:
        # Create a folder per basin for shp files
        shp_path = Path(dest_path, 'shapes', shape['bacia'])
        shp_path.mkdir(parents=True, exist_ok=True)
        # Convert bytes from database to geometry type
        geometry = Parser.bytes_to_geometry(bytes(shape['contorno']))
        #print(geometry)
        # Generate .shp file from shape
        geometry = geometry if geometry.IsValid() else geometry.Buffer(0)
        shp = Parser.geometry_to_file(geometry, shp_path, shape[f'nome_cotorno'])
        result.append({
            'macro_bacia': shape['bacia'],
            'sub_bacia': shape['sub_bacia'],
            'shape': shp
        })
    logger.info(f'A total of {len(result)} sub-basins data was read from database.')
    return result


def get_last_sync() -> datetime:
    """
    Retrieve datetime of last raster available
    :return: list of query result
    """
    try:
        database = os.environ['POSTGRES_DATABASE']
        table = os.environ['POSTGRES_RASTER_TABLE']
        time_column = 'data_hora' if os.environ['GRANULARITY'] == 'HOURLY' else 'data'
        read_statement = f'select max({time_column}) from {table};'
        db = Postgres(database=database)
        result = db.read(query=read_statement)
        if result:
            last_sync = result[0][0]
            if type(last_sync) == date:
                last_sync = datetime.combine(last_sync, time.min)
            return last_sync
    except psycopg2.errors.UndefinedColumn:
        raise ExtractionException('POSTGRES_RASTER_TABLE is not compatible with GRANULARITY')
