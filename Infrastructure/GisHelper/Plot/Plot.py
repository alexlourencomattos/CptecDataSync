from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
import math
import numpy as np


def plot_data(grads_file: GradsFile, variable_name: str, label: str) -> None:
    """
    Plot CTLReader Data
    :param grads_file: CTLReader Class Output
    """

    lat = grads_file.variables['latitude'].data
    lon = grads_file.variables['longitude'].data
    values = grads_file.variables[variable_name].data

    lat_max = max(lat)
    lat_min = min(lat)
    lon_max = max(lon)
    lon_min = min(lon)

    m = Basemap(llcrnrlon=lon_min, llcrnrlat=lat_min, urcrnrlon=lon_max, urcrnrlat=lat_max)

    # Draw the components of the map
    m.drawmapboundary(fill_color='#A6CAE0', linewidth=0)
    m.fillcontinents(color='black', alpha=0.6, lake_color='grey')
    m.drawcoastlines(linewidth=0.1, color="white")

    lon, lat = np.meshgrid(lon, lat)
    m.pcolormesh(lon, lat, values.reshape(lat.shape), latlon=True, cmap='RdBu_r')

    plt.clim(0, math.ceil(values.max()))
    plt.colorbar(label=label)

    plt.show()
