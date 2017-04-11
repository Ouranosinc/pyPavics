#!/usr/bin/python

import os
import sys
import ConfigParser

import numpy as np

import pavics.netcdf as pavnc
import pavics.synthetic as synt

synthetic_txt = 'Synthetic test data, not representative of actual events.'


def pairing_fx_testdata_grid1(output_path):
    # Regular lon lat grid (360x179); longitudes 0 to 360
    nc_file = os.path.join(output_path, 'pairing_fx_testdata_grid1.nc')
    global_attributes = {'Conventions': 'CF-1.6',
                         'comment': synthetic_txt}
    dimensions = [('lat', 179), ('lon', 360)]
    variables = {
        'pairing': {'create_args': {'datatype': 'f4',
                                    'dimensions': ('lat', 'lon'),
                                    'zlib': True,
                                    'chunksizes': None},
                    'attributes': {'units': '1',
                                   'standard_name': 'pairing_function'}}}
    nc = pavnc.create_dummy_netcdf(nc_file, 'NETCDF4_CLASSIC',
                                   global_attributes, dimensions, variables,
                                   close_nc=False)
    nc.variables['pairing'][:,:] = synt.mpairing((179, 360))
    nc.close()


def pairing_fx_testdata_grid2(output_path):
    # Regular lon lat grid (360x179); longitudes -180 to 180
    nc_file = os.path.join(output_path, 'pairing_fx_testdata_grid2.nc')
    global_attributes = {'Conventions': 'CF-1.6',
                         'comment': synthetic_txt}
    dimensions = [('lat', 179), ('lon', 360)]
    variables = {
        'pairing': {'create_args': {'datatype': 'f4',
                                    'dimensions': ('lat', 'lon'),
                                    'zlib': True,
                                    'chunksizes': None},
                    'attributes': {'units': '1',
                                   'standard_name': 'pairing_function'}}}
    lon_values = np.arange(-180.0, 180.0)
    nc = pavnc.create_dummy_netcdf(nc_file, 'NETCDF4_CLASSIC',
                                   global_attributes, dimensions, variables,
                                   lon_values=lon_values, close_nc=False)
    nc.variables['pairing'][:,:] = synt.mpairing((179, 360))
    nc.close()


def pairing_fx_testdata_grid3(output_path):
    from mpl_toolkits.basemap import Basemap
    # Irregular lon lat grid (172x180); longitudes -180 to 180
    nc_file = os.path.join(output_path, 'pairing_fx_testdata_grid3.nc')
    global_attributes = {'Conventions': 'CF-1.6',
                         'comment': synthetic_txt}
    dimensions = [('yc', 172), ('xc', 180)]
    variables = {
        'pairing': {'create_args': {'datatype': 'f4',
                                    'dimensions': ('yc', 'xc'),
                                    'zlib': True,
                                    'chunksizes': None},
                    'attributes': {'units': '1',
                                   'standard_name': 'pairing_function'}}}
    m = Basemap(projection='npstere', boundinglat=10, lon_0=270,
                resolution='l')
    xs = np.arange(2000000., 16400000., 80000)
    ys = np.arange(5000000., 10150000., 30000)
    x2d, y2d = np.meshgrid(xs, ys)
    lon_values, lat_values = m(x2d, y2d, inverse=True)
    nc = pavnc.create_dummy_netcdf(nc_file, 'NETCDF4_CLASSIC',
                                   global_attributes, dimensions, variables,
                                   lon_values=lon_values,
                                   lat_values=lat_values, close_nc=False)
    nc.variables['pairing'][:,:] = synt.mpairing((172, 180))
    nc.close()


def pairing_fx_testdata_stations1(output_path):
    # Station data
    nc_file = os.path.join(output_path, 'pairing_fx_testdata_stations1.nc')
    global_attributes = {'Conventions': 'CF-1.6',
                         'comment': synthetic_txt}
    dimensions = [('station', 100)]
    variables = {
        'pairing': {'create_args': {'datatype': 'f4',
                                    'dimensions': ('station'),
                                    'zlib': True,
                                    'chunksizes': None},
                    'attributes': {'units': '1',
                                   'standard_name': 'pairing_function'}}}
    nc = pavnc.create_dummy_netcdf(nc_file, 'NETCDF4_CLASSIC',
                                   global_attributes, dimensions, variables,
                                   close_nc=False)
    nc.variables['pairing'][:] = synt.mpairing((100,))
    nc.close()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        output_path = sys.argv[1]
    else:
        output_path = os.getcwd()
    config = ConfigParser.RawConfigParser()
    config.read('generate_pairing_data.ini')
    datasets = [x[0] for x in config.items('Datasets')]
    for dataset in datasets:
        locals()[dataset](output_path)
