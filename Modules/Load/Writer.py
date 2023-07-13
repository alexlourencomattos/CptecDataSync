from google.cloud.bigquery import LoadJobConfig, LoadJob
from Infrastructure.Postgres import Postgres
from Utils.Logger import get_logger
from google.cloud import bigquery
from datetime import datetime
import pandas as pd
import os


"""
Module to write the result of calculation to destine database.
"""
logger = get_logger(__name__)
__all__ = ['write_raster', 'write_basin_data']


def write_basin_data_to_bgq(precipitation_data: list) -> None:
    """
    Write the result of calculation to Google Big Query database
    :param precipitation_data: output of calculation
    """

    table = os.environ['GBQ_BASIN_DATA_TABLE']
    project_id = os.environ['GBQ_PROJECT_ID']
    dataset_location = os.environ['GBQ_DATASET_LOCATION']
    time_column = 'data_hora' if os.environ['GRANULARITY'] == 'HOURLY' else 'data'
    time_column_type = 'DATETIME' if os.environ['GRANULARITY'] == 'HOURLY' else 'DATE'
    schema = [
        {'name': time_column, 'type': time_column_type, 'mode': 'required'},
        {'name': 'macro_bacia', 'type': 'STRING', 'mode': 'required'},
        {'name': 'sub_bacia', 'type': 'STRING', 'mode': 'required'},
        {'name': 'valor', 'type': 'FLOAT', 'mode': 'required'},
        {'name': 'inserido_em', 'type': 'TIMESTAMP', 'mode': 'required'},
    ]
    df = pd.DataFrame(data=precipitation_data)
    df['inserido_em'] = datetime.now().timestamp()
    df.columns = [c['name'] for c in schema]
    df[time_column] = df[time_column].astype(str)
    rows = df.to_dict(orient='records')

    client = bigquery.Client(project=project_id, location=dataset_location)

    load_job_config: LoadJobConfig = LoadJobConfig(
        schema=schema,
        write_disposition='WRITE_APPEND',
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    )

    load_job: LoadJob = client.load_table_from_json(
        json_rows=rows,
        destination=table,
        location=dataset_location,
        job_config=load_job_config,
        job_id_prefix='CPTEC-MERGE-ETL'
    )

    load_job.result()


def write_basin_data_to_pg(precipitation_data: list) -> None:
    """
    Write the result of calculation to Google Big Query database
    :param precipitation_data: output of calculation
    """
  
    database = os.environ['POSTGRES_DATABASE']
    table = os.environ['POSTGRES_BASIN_DATA_TABLE']
    time_column = 'data_hora' if os.environ['GRANULARITY'] == 'HOURLY' else 'data'
    columns = (time_column, 'macro_bacia', 'sub_bacia', 'valor')
    # Build a multiline string with columns separated by pipe
    data = '\n'.join(['|'.join([str(c) for c in r.values()]) for r in precipitation_data])
    db = Postgres(database=database)
    db.bulk_insert(table=table, data=data, columns=columns, sep='|')


def write_raster_data_to_pg(data: list) -> None:
    """
    Insert data on SeriesTemporaisCptec
    """
    database = os.environ['POSTGRES_DATABASE']
    table = os.environ['POSTGRES_RASTER_TABLE']
    time_column = 'data_hora' if os.environ['GRANULARITY'] == 'HOURLY' else 'data'

    write_statement = f"""
    INSERT INTO {table}
    (
        {time_column},
        mapa
    ) 
    VALUES (%s, ST_FromGDALRaster(%s)) 
    ON CONFLICT ({time_column})
    DO UPDATE SET mapa = EXCLUDED.mapa;"""

    db = Postgres(database=database)
    db.insert(query=write_statement, params=data)


def write_raster(rasters: list) -> None:
    logger.info('Loading rasters on database')
    write_raster_data_to_pg(rasters)


def write_basin_data(precipitation_data: list):
    logger.info('Loading basins precipitation data on database')
    write_basin_data_to_bgq(precipitation_data)
    write_basin_data_to_pg(precipitation_data)
