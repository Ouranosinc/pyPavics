import unittest

import os
import netCDF4

import pavics.netcdf as pavnc

class TestNetCDF(unittest.TestCase):

    def setUp(self):
        self.dummy_file_1 = 'dummy_netcdf_file_1.nc'

    def tearDown(self):
        if os.path.isfile(self.dummy_file_1):
            os.remove(self.dummy_file_1)

    def test_period2indices_01(self):
        pavnc.create_dummy_netcdf(self.dummy_file_1,time_num_values=59,
                                  time_units='days since 2001-01-01 00:00:00',
                                  time_calendar='gregorian')
        d = pavnc.period2indices('2001-01-02T00:00:00',
                                 '2001-01-31T23:59:59',
                                 self.dummy_file_1)
        self.assertEqual(d['initial_index'],1)
        self.assertEqual(d['final_index'],30)

suite = unittest.TestLoader().loadTestsFromTestCase(TestNetCDF)

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite)
