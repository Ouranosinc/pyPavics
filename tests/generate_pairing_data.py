#!/usr/bin/python

import os
import sys
import ConfigParser

import pavics.netcdf as pavnc
import pavics.synthetic as synt

synthetic_txt = 'Synthetic test data, not representative of actual events.'


def pairing_fx_testdata_grid1(output_path):
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
