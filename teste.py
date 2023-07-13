from Infrastructure.Postgres import Postgres
from Infrastructure.GisHelper import Parser
from datetime import datetime, date, time
from Utils.Logger import get_logger
from pathlib import Path
import psycopg2
import os

#from Modules.Extraction.Exceptions import DataSource



database_name = os.environ['SHAPES_DATABASE']
db = Postgres(database=database_name)
data = {'codigo_ana': 'PSATIPSI', 'sub_bacia': 'Ilha dos Pombos', 'bacia': 'OUTRAS SUDESTE', 'centroide': '(-21,84, -42,58)', 'contorno_gefs': 'ILHAP', 'contorno_eta': 'ILHAP'}
db.bulk_insert(table= 'config', data=data)

print('teste')
