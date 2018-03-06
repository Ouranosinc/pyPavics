import os
import sys

import netCDF4


def create_path(output_path, relative_path):
    abs_path = os.path.join(output_path, os.path.dirname(relative_path))
    if not os.path.isdir(abs_path):
        os.makedirs(abs_path)
    return os.path.join(output_path, relative_path)


def create_dummy_netcdf(output_path):
    # First test file
    relative_path = 'TestProject01/test_project_01.nc'
    nc_file = create_path(output_path, relative_path)
    nc = netCDF4.Dataset(nc_file, 'w', format='NETCDF4_CLASSIC')
    nc.Conventions = 'CF-1.7'
    nc.title = 'Dummy NetCDF file'
    nc.history = '2018-03-06T19:00:00 - File structure established.'
    ncvar = nc.createVariable('test', 'i2', ())
    ncvar.units = '1'
    ncvar.long_name = 'Test variable'
    ncvar.standard_name = 'test_variable'
    ncvar[0] = 0
    nc.close()

    # Test file that should be private
    relative_path = 'TestPrivate01/test_private_01.nc'
    nc_file = create_path(output_path, relative_path)
    nc = netCDF4.Dataset(nc_file, 'w', format='NETCDF4_CLASSIC')
    nc.Conventions = 'CF-1.7'
    nc.title = 'Dummy NetCDF file'
    nc.history = '2018-03-06T19:30:00 - File structure established.'
    ncvar = nc.createVariable('test', 'i2', ())
    ncvar.units = '1'
    ncvar.long_name = 'Test variable'
    ncvar.standard_name = 'test_variable'
    ncvar[0] = 0
    nc.close()


if __name__ == '__main__':
    if len(sys.argv) > 1:
        create_dummy_netcdf(sys.argv[1])
    else:
        create_dummy_netcdf(os.getcwd())
