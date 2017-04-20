from past.builtins import basestring
import netCDF4

from . import geogrid


def _nearest_lon_lat(nclon, nclat, lon, lat, maximum_distance=None):
    nclon_data = nclon[...]
    nclat_data = nclat[...]
    grid_type = geogrid.detect_grid(nclon_data, nclat_data,
                                    lon_dimensions=nclon.dimensions,
                                    lat_dimensions=nclat.dimensions)
    indices = geogrid.find_nearest(nclon[...], nclat[...], lon, lat,
                                   maximum_distance, grid_type)
    d = {}
    if grid_type in geogrid.list_of_points:
        d[nclon.dimensions[0]] = indices
    elif grid_type in geogrid.rectilinear_grids:
        d[nclon.dimensions[0]] = indices[0]
        d[nclat.dimensions[0]] = indices[1]
    elif grid_type in geogrid.irregular_grids:
        d[nclon.dimensions[0]] = indices[0]
        d[nclon.dimensions[1]] = indices[1]
    return d


def nearest_lon_lat(nc_resource, lon, lat, maximum_distance=None):
    if isinstance(nc_resource, basestring):
        ncdataset = netCDF4.Dataset(nc_resource, 'r')
    elif isinstance(nc_resource, netCDF4._netCDF4.Dataset):
        ncdataset = nc_resource
    elif isinstance(nc_resource, (list, tuple)):
        ncdataset = netCDF4.Dataset(nc_resource[0], 'r')
    else:
        raise NotImplementedError()

    nclon = ncdataset.variables['lon']
    nclat = ncdataset.variables['lat']
    indices = _nearest_lon_lat(nclon, nclat, lon, lat, maximum_distance)

    if not isinstance(nc_resource, netCDF4._netCDF4.Dataset):
        ncdataset.close()
    return indices
