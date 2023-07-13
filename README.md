## CPTEC Merge Sync

This project is an ETL process of observed precipitation data, made available by CPTEC. 

The precipitation data are available in two different granularity, hourly and daily, and in grib2 format, commonly used 
in meteorology to store historical and forecast weather data. 

The project verifies CPTEC FTP server if there is a new file available. The file is processed and ingested on database. 
The files processing consists in extract from each file the average precipitation per sub-basin, what is done using a 
crop technique, with sub-basins shapes made available by ONS.


## Setup

This project use `GDAL`,a translator library for raster and vector geospatial data. Before run you need make the library
setup on your system. Because it is a C++ core library the setup can be a little bit complex and with many steps. The follow
tutorials can be helpful:

* [Windows Tutorial](https://sandbox.idre.ucla.edu/sandbox/tutorials/installing-gdal-for-windows)
* [Linux tutorial](https://mothergeo-py.readthedocs.io/en/latest/development/how-to/gdal-ubuntu-pkg.html)

Besides of GDAL there are some other dependencies that must be installed. for this, just execute following command: 

```
pip install -r requiretements.txt
```

Finally, the final setup step is declare some environment variables, used to define the database credentials, CPTEC
Server path and other config values. Follow the list of them:
 

| Variable | Description | Example |
|   ---    |     ---     |   ---   |
|POSTGRE_HOST| Postgres Server host| postgres|
|POSTGRE_PORT| Postgres Server port| 5782|
|POSTGRE_USERNAME| Postgres username credential| --- |
|POSTGRE_PASSWORD| Postgres password credential| --- |
|FTP_HOST| CPTEC FTP Server host| ftp.cptec.inpe.br | 
|CPTEC_FTP_DIR| Path to merge files on FTP Server| /modelos/tempo/MERGE/GPM | 
|GRANULARITY| Granularity of data to be collected (HOURLY, DAILY)| HOURLY |
|MODEL| Model that the shapes will be used to crop data (ETA40 or GEFS)| ETA40 |
|SHAPES_DATABASE| Name of database with shapes|contornos|
|GOOGLE_APPLICATION_CREDENTIALS| Path to .json file with Google Cloud Credentials|/run/secrets/credential.json|
|GBQ_PROJECT_ID| Google Big Query project id|dataset|
|GBQ_BASIN_DATA_TABLE| Google Big Query ingest table|Cptec.precipitacao_horaria_sub_bacia| 
|GBQ_DATASET_LOCATION| Google Big Query table location|southamerica-east1|
|POSTGRES_DATABASE| Name of database to store CPTEC Data|SeriesTemporaisCptec|
|POSTGRES_RASTER_TABLE| Name of table to store the unprocessed data|series_horarias.gpm_merge| 
|POSTGRES_BASIN_DATA_TABLE| Name of table to store the unprocessed data|series_horarias.precipitacao_bacia|    
|SKIP_WRITE_RASTER| Skip unprocessed data (raster) ingest|TRUE|
|SYNC_FROM| Define the initial range of data to be processed|2020-07-07h09|
|SYNC_TO| Define the end range of data to be processed|2020-07-07h12|
|PROCESSES| Number of simultaneously process on multi-threading|3|

The `SYNC_FROM` and `SYNC_TO`variables are optional. If not defined the last sync date and current date time will be 
assumed respectively.

The data processed by this project is used to feed some Power Bi Dashboards and for this reason has a trigger to
update the dashboard after data ingest. To use this functions this variables bellow must be declared too: 

| Variable | Description | Example | 
|   ---    |     ---     |   ---   |
|POWERBI_REFRESH|Boolean to activate the function|TRUE| 
|POWERBI_REFRESH_API_URL|API URL|http://powerbi-refresher.dominio.ws|
|POWERBI_REFRESH_WORKSPACE|Power Bi workspace name| workspace|
|POWERBI_REFRESH_DATASET|Power Bi dataset name|Chuva|

## Run

Finally, if all setup steps was followed as described you are ready to execute the application. To execute is very simple,
been necessary just call the [main.py](main.py) module, as example bellow: 

```
python main.py
```




