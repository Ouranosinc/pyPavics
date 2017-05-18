import unittest
import os

import numpy as np
import netCDF4
from shapely.geometry import Polygon

import pavics.netcdf as pavnc
import pavics.geogrid as geogrid
import pavics.ncgeo as ncgeo


class TestNcGeo(unittest.TestCase):

    def setUp(self):
        self.dummy_file_1 = 'dummy_netcdf_file_1.nc'
        self.dummy_file_2 = 'dummy_netcdf_file_2.nc'

    def tearDown(self):
        if os.path.isfile(self.dummy_file_1):
            os.remove(self.dummy_file_1)
        if os.path.isfile(self.dummy_file_2):
            os.remove(self.dummy_file_2)

    def test_reshape_subdomain_01(self):
        pavnc.create_dummy_netcdf(self.dummy_file_1, use_time=False,
                                  lat_size=5, lon_size=6,
                                  lon_values=[100, 110, 120, 130, 140, 150],
                                  lat_values=[10, 20, 30, 40, 50],
                                  var_values=[[0, 1, 2, 3, 4, 5],
                                              [10, 11, 12, 13, 14, 15],
                                              [20, 21, 22, 23, 24, 25],
                                              [30, 31, 32, 33, 34, 35],
                                              [40, 41, 42, 43, 44, 45]])
        nc = netCDF4.Dataset(self.dummy_file_1, 'r')
        nclon = nc.variables['lon']
        nclat = nc.variables['lat']
        geometry = Polygon([(116, 26), (116, 44), (141, 44), (141, 26)])
        reshapes = ncgeo.reshape_subdomain(nclon, nclat, geometry)
        self.assertEqual(reshapes, {'lon': 3, 'lat': 2})

    def test_reshape_subdomain_02(self):
        pavnc.create_dummy_netcdf(self.dummy_file_1, use_time=False,
                                  use_lat=False, use_lon=False,
                                  use_station=True, station_size=5,
                                  lon_values=[100, 110, 120, 130, 140],
                                  lat_values=[10, 20, 30, 40, 50],
                                  var_values=[0, 1, 2, 3, 4])
        nc = netCDF4.Dataset(self.dummy_file_1, 'r')
        nclon = nc.variables['lon']
        nclat = nc.variables['lat']
        geometry = Polygon([(106, 16), (106, 44), (141, 44), (141, 16)])
        reshapes = ncgeo.reshape_subdomain(nclon, nclat, geometry)
        self.assertEqual(reshapes, {'station': 3})

    def test_reshape_subdomain_03(self):
        mylon = np.array([[120, 160, 210, 270, 300, 330],
                          [130, 170, 210, 280, 310, 330],
                          [130, 170, 210, 280, 310, 330],
                          [140, 170, 220, 290, 310, 330],
                          [160, 190, 240, 290, 310, 340]])
        mylat = np.array([[13, 14, 15, 17, 18, 20],
                          [17, 18, 19, 21, 22, 24],
                          [21, 22, 23, 24, 26, 27],
                          [26, 27, 28, 29, 30, 32],
                          [30, 32, 34, 36, 38, 39]])
        pavnc.create_dummy_netcdf(self.dummy_file_1, use_time=False,
                                  use_lat=False, use_lon=False,
                                  use_ycxc=True, yc_size=5, xc_size=6,
                                  lon_values=mylon, lat_values=mylat,
                                  var_values=[[0, 1, 2, 3, 4, 5],
                                              [10, 11, 12, 13, 14, 15],
                                              [20, 21, 22, 23, 24, 25],
                                              [30, 31, 32, 33, 34, 35],
                                              [40, 41, 42, 43, 44, 45]])
        nc = netCDF4.Dataset(self.dummy_file_1, 'r')
        nclon = nc.variables['lon']
        nclat = nc.variables['lat']
        geometry = Polygon([(160, 19), (160, 24), (290, 25), (290, 22)])
        reshapes = ncgeo.reshape_subdomain(nclon, nclat, geometry)
        self.assertEqual(reshapes, {'xc': 3, 'yc': 2})

    def test_reshape_subdomain_04(self):
        mylon = np.array([[3, 7, 11, 15, 19, 23],
                          [5, 9, 13, 17, 21, 25],
                          [7, 11, 15, 19, 23, 27],
                          [9, 13, 17, 21, 25, 29],
                          [11, 15, 19, 23, 27, 31]])
        mylat = np.array([[2.5, 3.5, 4.5, 5.5, 6.5, 7.5],
                          [6.5, 7.5, 8.5, 9.5, 10.5, 11.5],
                          [10.5, 11.5, 12.5, 13.5, 14.5, 16.5],
                          [14.5, 15.5, 16.5, 17.5, 18.5, 19.5],
                          [18.5, 19.5, 20.5, 21.5, 22.5, 23.5]])
        pavnc.create_dummy_netcdf(self.dummy_file_1, use_time=False,
                                  use_lat=False, use_lon=False,
                                  use_ycxc=True, yc_size=5, xc_size=6,
                                  lon_values=mylon, lat_values=mylat,
                                  var_values=[[0, 1, 2, 3, 4, 5],
                                              [10, 11, 12, 13, 14, 15],
                                              [20, 21, 22, 23, 24, 25],
                                              [30, 31, 32, 33, 34, 35],
                                              [40, 41, 42, 43, 44, 45]])
        nc = netCDF4.Dataset(self.dummy_file_1, 'r')
        nclon = nc.variables['lon']
        nclat = nc.variables['lat']
        geometry = Polygon([(0.1, 0.1), (0.1, 0.2), (0.2, 0.2), (0.2, 0.1)])
        reshapes = ncgeo.reshape_subdomain(nclon, nclat, geometry)
        self.assertEqual(reshapes, {'xc': 1, 'yc': 1})

    def test_reshape_subdomain_05(self):
        mylon = np.array([[3, 7, 11, 15, 19, 23],
                          [5, 9, 13, 17, 21, 25],
                          [7, 11, 15, 19, 23, 27],
                          [9, 13, 17, 21, 25, 29],
                          [11, 15, 19, 23, 27, 31]])
        mylat = np.array([[2.5, 3.5, 4.5, 5.5, 6.5, 7.5],
                          [6.5, 7.5, 8.5, 9.5, 10.5, 11.5],
                          [10.5, 11.5, 12.5, 13.5, 14.5, 16.5],
                          [14.5, 15.5, 16.5, 17.5, 18.5, 19.5],
                          [18.5, 19.5, 20.5, 21.5, 22.5, 23.5]])
        pavnc.create_dummy_netcdf(self.dummy_file_1, use_time=False,
                                  use_lat=False, use_lon=False,
                                  use_ycxc=True, yc_size=5, xc_size=6,
                                  lon_values=mylon, lat_values=mylat,
                                  var_values=[[0, 1, 2, 3, 4, 5],
                                              [10, 11, 12, 13, 14, 15],
                                              [20, 21, 22, 23, 24, 25],
                                              [30, 31, 32, 33, 34, 35],
                                              [40, 41, 42, 43, 44, 45]])
        nc = netCDF4.Dataset(self.dummy_file_1, 'r')
        nclon = nc.variables['lon']
        nclat = nc.variables['lat']
        geometry = Polygon([(-0.1, -0.1), (-0.1, -0.2), (-0.2, -0.2),
                            (-0.2, -0.1)])
        self.assertRaises(geogrid.GeogridError, ncgeo.reshape_subdomain,
                          nclon, nclat, geometry)

    def test_reshape_subdomain_06(self):
        mylon = np.array([[3, 7, 11, 15, 19, 23],
                          [5, 9, 13, 17, 21, 25],
                          [7, 11, 15, 19, 23, 27],
                          [9, 13, 17, 21, 25, 29],
                          [11, 15, 19, 23, 27, 31]])
        mylat = np.array([[2.5, 3.5, 4.5, 5.5, 6.5, 7.5],
                          [6.5, 7.5, 8.5, 9.5, 10.5, 11.5],
                          [10.5, 11.5, 12.5, 13.5, 14.5, 16.5],
                          [14.5, 15.5, 16.5, 17.5, 18.5, 19.5],
                          [18.5, 19.5, 20.5, 21.5, 22.5, 23.5]])
        pavnc.create_dummy_netcdf(self.dummy_file_1, use_time=False,
                                  use_lat=False, use_lon=False,
                                  use_ycxc=True, yc_size=5, xc_size=6,
                                  lon_values=mylon, lat_values=mylat,
                                  var_values=[[0, 1, 2, 3, 4, 5],
                                              [10, 11, 12, 13, 14, 15],
                                              [20, 21, 22, 23, 24, 25],
                                              [30, 31, 32, 33, 34, 35],
                                              [40, 41, 42, 43, 44, 45]])
        nc = netCDF4.Dataset(self.dummy_file_1, 'r')
        nclon = nc.variables['lon']
        nclat = nc.variables['lat']
        geometry = Polygon([(30.2, 24.1), (30.2, 24.2), (30.3, 24.2),
                            (30.3, 24.1)])
        reshapes = ncgeo.reshape_subdomain(nclon, nclat, geometry)
        self.assertEqual(reshapes, {'xc': 1, 'yc': 1})

    def test_reshape_subdomain_07(self):
        mylon = np.array([[3, 7, 11, 15, 19, 23],
                          [5, 9, 13, 17, 21, 25],
                          [7, 11, 15, 19, 23, 27],
                          [9, 13, 17, 21, 25, 29],
                          [11, 15, 19, 23, 27, 31]])
        mylat = np.array([[2.5, 3.5, 4.5, 5.5, 6.5, 7.5],
                          [6.5, 7.5, 8.5, 9.5, 10.5, 11.5],
                          [10.5, 11.5, 12.5, 13.5, 14.5, 16.5],
                          [14.5, 15.5, 16.5, 17.5, 18.5, 19.5],
                          [18.5, 19.5, 20.5, 21.5, 22.5, 23.5]])
        pavnc.create_dummy_netcdf(self.dummy_file_1, use_time=False,
                                  use_lat=False, use_lon=False,
                                  use_ycxc=True, yc_size=5, xc_size=6,
                                  lon_values=mylon, lat_values=mylat,
                                  var_values=[[0, 1, 2, 3, 4, 5],
                                              [10, 11, 12, 13, 14, 15],
                                              [20, 21, 22, 23, 24, 25],
                                              [30, 31, 32, 33, 34, 35],
                                              [40, 41, 42, 43, 44, 45]])
        nc = netCDF4.Dataset(self.dummy_file_1, 'r')
        nclon = nc.variables['lon']
        nclat = nc.variables['lat']
        geometry = Polygon([(8.01, 9.01), (10.01, 12.99),
                            (13.99, 13.99), (11.99, 10.01)])
        reshapes = ncgeo.reshape_subdomain(nclon, nclat, geometry)
        self.assertEqual(reshapes, {'xc': 1, 'yc': 1})

    def test_reshape_subdomain_08(self):
        mylon = np.array([[3, 7, 11, 15, 19, 23],
                          [5, 9, 13, 17, 21, 25],
                          [7, 11, 15, 19, 23, 27],
                          [9, 13, 17, 21, 25, 29],
                          [11, 15, 19, 23, 27, 31]])
        mylat = np.array([[2.5, 3.5, 4.5, 5.5, 6.5, 7.5],
                          [6.5, 7.5, 8.5, 9.5, 10.5, 11.5],
                          [10.5, 11.5, 12.5, 13.5, 14.5, 16.5],
                          [14.5, 15.5, 16.5, 17.5, 18.5, 19.5],
                          [18.5, 19.5, 20.5, 21.5, 22.5, 23.5]])
        pavnc.create_dummy_netcdf(self.dummy_file_1, use_time=False,
                                  use_lat=False, use_lon=False,
                                  use_ycxc=True, yc_size=5, xc_size=6,
                                  lon_values=mylon, lat_values=mylat,
                                  var_values=[[0, 1, 2, 3, 4, 5],
                                              [10, 11, 12, 13, 14, 15],
                                              [20, 21, 22, 23, 24, 25],
                                              [30, 31, 32, 33, 34, 35],
                                              [40, 41, 42, 43, 44, 45]])
        nc = netCDF4.Dataset(self.dummy_file_1, 'r')
        nclon = nc.variables['lon']
        nclat = nc.variables['lat']
        geometry = Polygon([(7.99, 8.99), (9.99, 13.01),
                            (14.01, 14.01), (12.01, 9.99)])
        reshapes = ncgeo.reshape_subdomain(nclon, nclat, geometry)
        self.assertEqual(reshapes, {'xc': 3, 'yc': 3})

    def test_subset_bbox_01(self):
        pavnc.create_dummy_netcdf(self.dummy_file_1, use_time=False,
                                  lat_size=5, lon_size=6,
                                  lon_values=[100, 110, 120, 130, 140, 150],
                                  lat_values=[10, 20, 30, 40, 50],
                                  var_values=[[0, 1, 2, 3, 4, 5],
                                              [10, 11, 12, 13, 14, 15],
                                              [20, 21, 22, 23, 24, 25],
                                              [30, 31, 32, 33, 34, 35],
                                              [40, 41, 42, 43, 44, 45]])
        ncgeo.subset_bbox(self.dummy_file_1, self.dummy_file_2,
                          [116, 16, 141, 44])
        nc = netCDF4.Dataset(self.dummy_file_2, 'r')
        nclon = nc.variables['lon']
        nclat = nc.variables['lat']
        ncvar = nc.variables['dummy']
        self.assertEqual(nclon.size, 3)
        self.assertEqual(nclat.size, 3)
        self.assertEqual(nclon[0], 120)
        self.assertEqual(nclon[-1], 140)
        self.assertEqual(nclat[0], 20)
        self.assertEqual(nclat[-1], 40)
        self.assertEqual(ncvar.shape, (3, 3))
        self.assertEqual(ncvar[0,0], 12)
        self.assertEqual(ncvar[-1,-1], 34)

    def test_subset_bbox_02(self):
        pavnc.create_dummy_netcdf(self.dummy_file_1, use_time=False,
                                  use_lat=False, use_lon=False,
                                  use_station=True, station_size=5,
                                  lon_values=[1, 6, 3, 7, 0],
                                  lat_values=[6, 5, 3, 2, 0],
                                  var_values=[0, 1, 2, 3, 4])
        ncgeo.subset_bbox(self.dummy_file_1, self.dummy_file_2,
                          [5, 1, 8, 6])
        nc = netCDF4.Dataset(self.dummy_file_2, 'r')
        nclon = nc.variables['lon']
        nclat = nc.variables['lat']
        ncvar = nc.variables['dummy']
        self.assertEqual(nclon.size, 2)
        self.assertEqual(nclat.size, 2)
        self.assertEqual(nclon[0], 6)
        self.assertEqual(nclon[1], 7)
        self.assertEqual(nclat[0], 5)
        self.assertEqual(nclat[1], 2)
        self.assertEqual(ncvar.shape, (2,))
        self.assertEqual(ncvar[0], 1)
        self.assertEqual(ncvar[1], 3)

    def test_subset_bbox_03(self):
        mylon = np.array([[3, 7, 11, 15, 19, 23],
                          [5, 9, 13, 17, 21, 25],
                          [7, 11, 15, 19, 23, 27],
                          [9, 13, 17, 21, 25, 29],
                          [11, 15, 19, 23, 27, 31]])
        mylat = np.array([[2.5, 3.5, 4.5, 5.5, 6.5, 7.5],
                          [6.5, 7.5, 8.5, 9.5, 10.5, 11.5],
                          [10.5, 11.5, 12.5, 13.5, 14.5, 16.5],
                          [14.5, 15.5, 16.5, 17.5, 18.5, 19.5],
                          [18.5, 19.5, 20.5, 21.5, 22.5, 23.5]])
        pavnc.create_dummy_netcdf(self.dummy_file_1, use_time=False,
                                  use_lat=False, use_lon=False,
                                  use_ycxc=True, yc_size=5, xc_size=6,
                                  lon_values=mylon, lat_values=mylat,
                                  var_values=[[0, 1, 2, 3, 4, 5],
                                              [10, 11, 12, 13, 14, 15],
                                              [20, 21, 22, 23, 24, 25],
                                              [30, 31, 32, 33, 34, 35],
                                              [40, 41, 42, 43, 44, 45]])
        ncgeo.subset_bbox(self.dummy_file_1, self.dummy_file_2,
                          [17, 10, 21, 16])
        nc = netCDF4.Dataset(self.dummy_file_2, 'r')
        nclon = nc.variables['lon']
        nclat = nc.variables['lat']
        ncvar = nc.variables['dummy']
        self.assertEqual(nclon.shape, (3, 3))
        self.assertEqual(nclat.shape, (3, 3))
        self.assertEqual(nclon[0,0], 13)
        self.assertEqual(nclon[2,2], 25)
        self.assertEqual(nclat[0,0], 8.5)
        self.assertEqual(nclat[2,2], 18.5)
        self.assertEqual(ncvar.shape, (3, 3))
        self.assertEqual(ncvar[0,0], 12)
        self.assertEqual(ncvar[2,2], 34)

suite = unittest.TestLoader().loadTestsFromTestCase(TestNcGeo)

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite)
