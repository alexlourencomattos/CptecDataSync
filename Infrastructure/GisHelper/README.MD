# GisHelper

This project is an implementation of [GDAL(Geospatial Data Abstraction Library)](https://pypi.org/project/GDAL/) that aiming
turn easier work with Geospatial Data  bringing the main and more common functions to process shapes, geometries, rasters 
and PostGis structures.

## Setup 

This project use `GDAL`,a translator library for raster and vector geospatial data. For run it you need make the library
setup on your system. To do the setup on Windows, follow this [tutorial](https://sandbox.idre.ucla.edu/sandbox/tutorials/installing-gdal-for-windows).
For unix System (Based on Debian/Ubuntu) this steps can be followed:

```console
# Update base container install
apt-get update
apt-get upgrade -y

# Add repo to allow us to access latest GDAL builds
echo deb http://ftp.uk.debian.org/debian stable main >> /etc/apt/sources.list
apt-get update

# Install GDAL dependencies
apt-get install -y libgdal-dev

#Install BASEMAP dependencies
apt-get install libgeos-dev

# Update C env vars so compiler can find gdal
export CPLUS_INCLUDE_PATH=/usr/include/gdal
export C_INCLUDE_PATH=/usr/include/gdal

#Install GDAL and BASEMAP libraries
pip install numpy
pip install GDAL==$(gdal-config --version | awk -F'[.]' '{print $1"."$2}')
pip install --user https://github.com/matplotlib/basemap/archive/master.zip
``` 

* Obs: 
    * The steps above required Python already configured.
    * Numpy and GDAL python libraries must be installed.
    * maybe it is necessary specify a specific version of GDAL library.

## Usage

To use this project is very simple, you just need import the module and use the available functions bellow:

* **Parser(GisHelper.Parser.Parse)**:
   
    * grib_to_raster: convert a [.GRIB file](http://cola.gmu.edu/grads/gadoc/descriptorfile.html) to raster file (.tiff)
    
    * text_to_raster: Convert a text file (lat, lon, value) to a raster file (.tiff)  
 
    * bytes_to_raster: convert a [PostgisSql geometry](http://postgis.net/workshops/postgis-intro/geometries.html) bytes to raster file (.tiff)
        
    * raster_to_bytes: convert a raster file (.tiff) to bytes.
    
    * bytes_to_geometry: convert a [PostgisSql geometry](http://postgis.net/workshops/postgis-intro/geometries.html) bytes to gdal.Geometry Object
    
    * geometry_to_file: convert a [PostgisSql geometry](http://postgis.net/workshops/postgis-intro/geometries.html) bytes to shape file (.shp)
    
* **Calculate(GisHelper.Calculate.RasterCalculate)**:
    
    * calc: execute an arithmetical operation (sum, subtraction, etc) between rasters. 
    
    * mean: calculate the mean of all grid points available on raster
    
    * sum:  sum all grid points (at same position) of rasters listed
