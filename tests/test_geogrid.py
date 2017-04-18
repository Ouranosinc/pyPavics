import unittest

from testfixtures import LogCapture
import numpy as np
from shapely.geometry import Polygon, MultiPolygon

from pavics import geogrid


class TestGeogrid(unittest.TestCase):
    def setUp(self):
        self.poly_between_minus_180_0 = Polygon(
            [(-60, 20), (-64, 43), (-42, 59), (-43, 12)])
        self.poly_between_0_180 = Polygon(
            [(20, 20), (22, 43), (47, 59), (43, 12)])
        self.poly_between_180_360 = Polygon(
            [(243, 20), (240, 33), (320, 59), (305, 23)])
        self.poly_cross_0 = Polygon(
            [(-20, 10), (-15, 43), (10, 59), (23, 11)])
        self.poly_cross_180 = Polygon(
            [(165, 10), (170, 43), (195, 59), (205, 11)])
        self.poly_cross_360 = MultiPolygon(
            [(((340, 10), (340, 20), (360, 20), (360, 10)), []),
             (((0, 10), (0, 20), (10, 20), (13, 13)), [])])
        self.poly_cross_0_and_180 = MultiPolygon(
            [(((-20, 10), (-20, 20), (180, 20), (180, 10)), []),
             (((-180, 10), (-180, 20), (-160, 23), (-157, 12)), [])])
        self.poly_cross_180_and_360 = MultiPolygon(
            [(((50, 10), (80, 20), (360, 25), (360, 5)), []),
             (((0, 5), (0, 25), (10, 20), (13, 13)), [])])
        self.poly_invalid_lat = Polygon(
            [(40, -90.000000001), (45, -80), (85, -70), (90, -90.000000001)])
        self.poly_zonal_band_from_minus_180 = Polygon(
            [(-180, 10), (-180, 43), (180, 43), (180, 10)])
        self.poly_zonal_band_from_0 = Polygon(
            [(0, 10), (0, 43), (360, 43), (360, 10)])
        self.poly_zonal_band_from_180 = MultiPolygon(
            [(((180, 10), (180, 20), (360, 20), (360, 10)), []),
             (((0, 10), (0, 20), (180, 20), (180, 10)), [])])
        self.poly_zonal_band_from_44 = MultiPolygon(
            [(((44, 10), (44, 20), (360, 20), (360, 10)), []),
             (((0, 10), (0, 20), (44, 20), (44, 10)), [])])
        self.poly_globe_from_minus_180 = Polygon(
            [(-180, -90), (-180, 90), (180, 90), (180, -90)])
        self.poly_globe_from_0 = Polygon(
            [(0, -90), (0, 90), (360, 90), (360, -90)])
        self.poly_globe_from_180 = MultiPolygon(
            [(((180, -90), (180, 90), (360, 90), (360, -90)), []),
             (((0, -90), (0, 90), (180, 90), (180, -90)), [])])
        self.poly_globe_from_44 = MultiPolygon(
            [(((44, -90), (44, 90), (360, 90), (360, -90)), []),
             (((0, -90), (0, 90), (44, 90), (44, -90)), [])])
        self.poly_pole_from_minus_180 = Polygon(
            [(-180, -90), (-180, -80), (180, -80), (180, -90)])
        self.poly_pole_from_0 = Polygon(
            [(0, -90), (0, -80), (360, -80), (360, -90)])
        self.poly_pole_from_180 = MultiPolygon(
            [(((180, -90), (180, -80), (360, -80), (360, -90)), []),
             (((0, -90), (0, -80), (180, -80), (180, -90)), [])])
        self.poly_pole_from_44 = MultiPolygon(
            [(((44, -90), (44, -80), (360, -80), (360, -90)), []),
             (((0, -90), (0, -80), (44, -80), (44, -90)), [])])

    def tearDown(self):
        pass

    def test_find_nearest_from_list_of_points_01(self):
        lon_centroids = np.array([1, 1, 4, 7, 8])
        lat_centroids = np.array([1, 4, 5, 3, 1])
        lon_point = 3
        lat_point = 2
        i = geogrid._find_nearest_from_list_of_points(
            lon_centroids, lat_centroids, lon_point, lat_point)
        self.assertEqual(i, 0)

    def test_find_nearest_from_list_of_points_02(self):
        # Collocated points.
        lon_centroids = np.array([1, 1, 4, 3, 8])
        lat_centroids = np.array([1, 4, 5, 2, 1])
        lon_point = 3
        lat_point = 2
        i = geogrid._find_nearest_from_list_of_points(
            lon_centroids, lat_centroids, lon_point, lat_point)
        self.assertEqual(i, 3)

    def test_find_nearest_from_list_of_points_03(self):
        # More than one nearest point.
        lon_centroids = np.array([1, 1, 5, 7, 8])
        lat_centroids = np.array([1, 4, 1, 3, 1])
        lon_point = 3
        lat_point = 2
        with LogCapture() as l:
            i = geogrid._find_nearest_from_list_of_points(
                lon_centroids, lat_centroids, lon_point, lat_point)
        msg = 'More than one nearest point, returning first index.'
        l.check(('root', 'WARNING', msg))
        self.assertEqual(i, 0)

    def test_find_nearest_from_list_of_points_04(self):
        # Over maximum distance.
        lon_centroids = np.array([1, 1, 4, 7, 8])
        lat_centroids = np.array([1, 4, 5, 3, 1])
        lon_point = 3
        lat_point = 2
        self.assertRaises(geogrid.GeogridError,
                          geogrid._find_nearest_from_list_of_points,
                          lon_centroids, lat_centroids, lon_point, lat_point,
                          200000)

    def test_find_nearest_from_rectilinear_centroids_01(self):
        lon_centroids = np.array([0, 1, 2, 3, 4, 5, 6, 7])
        lat_centroids = np.array([0, 1, 2, 3, 4, 5])
        lon_point = 2.2
        lat_point = 3.9
        (i, j) = geogrid._find_nearest_from_rectilinear_centroids(
            lon_centroids, lat_centroids, lon_point, lat_point)
        self.assertEqual((i, j), (2, 4))

    def test_find_nearest_from_rectilinear_centroids_02(self):
        # Collocated point with centroids.
        lon_centroids = np.array([0, 1, 2, 3, 4, 5, 6, 7])
        lat_centroids = np.array([0, 1, 2, 3, 4, 5])
        lon_point = 4
        lat_point = 1
        (i, j) = geogrid._find_nearest_from_rectilinear_centroids(
            lon_centroids, lat_centroids, lon_point, lat_point)
        self.assertEqual((i, j), (4, 1))

    def test_find_nearest_from_rectilinear_centroids_03(self):
        # More than one nearest point
        lon_centroids = np.array([0, 1, 2, 3, 4, 5, 6, 7])
        lat_centroids = np.array([0, 1, 2, 3, 4, 5])
        lon_point = 2.5
        lat_point = 1.5
        with LogCapture() as l:
            (i, j) = geogrid._find_nearest_from_rectilinear_centroids(
                lon_centroids, lat_centroids, lon_point, lat_point)
        msg1 = 'More than one nearest meridian, returning first index.'
        msg2 = 'More than one nearest parallel, returning first index.'
        l.check(('root', 'WARNING', msg1), ('root', 'WARNING', msg2))
        self.assertEqual((i, j), (2, 1))

    def test_find_nearest_from_rectilinear_centroids_04(self):
        # Over maximum distance.
        lon_centroids = np.array([0, 1, 2, 3, 4, 5, 6, 7])
        lat_centroids = np.array([0, 1, 2, 3, 4, 5])
        lon_point = 2.6
        lat_point = 1.4
        self.assertRaises(geogrid.GeogridError,
                          geogrid._find_nearest_from_rectilinear_centroids,
                          lon_centroids, lat_centroids, lon_point, lat_point,
                          2000)

    def test_find_nearest_from_rectilinear_centroids_05(self):
        # Longitude expressed in different reference systems.
        lon_centroids = np.array([180, 181, 182, 183, 184, 185, 186, 187])
        lat_centroids = np.array([0, 1, 2, 3, 4, 5])
        lon_point = -174.9
        lat_point = 1
        (i, j) = geogrid._find_nearest_from_rectilinear_centroids(
            lon_centroids, lat_centroids, lon_point, lat_point)
        self.assertEqual((i, j), (5, 1))

    def test_find_nearest_from_rectilinear_centroids_06(self):
        # Longitude expressed in different reference systems.
        lon_centroids = np.array([180, 181, 182, 183, 184, 185, 186, 187])
        lat_centroids = np.array([0, 1, 2, 3, 4, 5])
        lon_point = -175.1
        lat_point = 1
        (i, j) = geogrid._find_nearest_from_rectilinear_centroids(
            lon_centroids, lat_centroids, lon_point, lat_point)
        self.assertEqual((i, j), (5, 1))

    def test_find_nearest_from_irregular_centroids_01(self):
        lon_centroids = np.array([[0, 3, 6], [1, 5, 9], [3, 7, 10],
                                  [6, 10, 12]])
        lat_centroids = np.array([[0, 1, 2], [2, 3, 3], [4, 4, 4], [6, 6, 6]])
        lon_point = 7.6
        lat_point = 4.2
        (i, j) = geogrid._find_nearest_from_irregular_centroids(lon_centroids,
                                                                lat_centroids,
                                                                lon_point,
                                                                lat_point)
        self.assertEqual((i, j), (2, 1))

    def test_find_nearest_from_irregular_centroids_02(self):
        # Collocated point with centroids
        lon_centroids = np.array([[0, 3, 6], [1, 5, 9], [3, 7, 10],
                                  [6, 10, 12]])
        lat_centroids = np.array([[0, 1, 2], [2, 3, 3], [4, 4, 4], [6, 6, 6]])
        lon_point = 6
        lat_point = 5
        (i, j) = geogrid._find_nearest_from_irregular_centroids(lon_centroids,
                                                                lat_centroids,
                                                                lon_point,
                                                                lat_point)
        self.assertEqual((i, j), (3, 0))

    def test_find_nearest_from_irregular_centroids_03(self):
        # More than one nearest point
        lon_centroids = np.array(
            [[0, 3, 6], [1, 5, 9], [3, 7, 10], [6, 10, 12]])
        lat_centroids = np.array([[0, 1, 2], [2, 3, 3], [4, 4, 4], [6, 6, 6]])
        lon_point = 3
        lat_point = 2.5
        with LogCapture() as l:
            (i, j) = geogrid._find_nearest_from_irregular_centroids(
                lon_centroids, lat_centroids, lon_point, lat_point)
        msg = 'More than one nearest point, returning first index.'
        l.check(('root', 'WARNING', msg))
        self.assertEqual((i, j), (0, 1))

    def test_find_nearest_from_irregular_centroids_04(self):
        # Over distance
        lon_centroids = np.array(
            [[0, 3, 6], [1, 5, 9], [3, 7, 10], [6, 10, 12]])
        lat_centroids = np.array([[0, 1, 2], [2, 3, 3], [4, 4, 4], [6, 6, 6]])
        lon_point = 3
        lat_point = 2.6
        self.assertRaises(geogrid.GeogridError,
                          geogrid._find_nearest_from_irregular_centroids,
                          lon_centroids, lat_centroids, lon_point, lat_point,
                          2000)

    def test_find_nearest_from_irregular_vertices_01(self):
        lon_vertices = np.array([[0, 3, 6, 9], [1, 5, 8, 11], [3, 7, 9, 12],
                                 [6, 10, 12, 14], [7, 11, 14, 16]])
        lat_vertices = np.array([[0, 1, 2, 2], [2, 3, 3, 3], [4, 4, 4, 4],
                                 [5, 5, 5, 5], [7, 8, 7, 6]])
        lon_point = 9.8
        lat_point = 5.1
        (i, j) = geogrid._find_nearest_from_irregular_vertices(
            lon_vertices, lat_vertices, lon_point, lat_point)
        self.assertEqual((i, j), (2, 1))

    def test_rectilinear_2d_bounds_to_vertices_01(self):
        lon_bnds = np.array([[1, 2], [2, 3], [3, 4]])
        lat_bnds = np.array([[10, 12], [12, 13]])
        f = geogrid.rectilinear_2d_bounds_to_vertices
        (lon_vertices, lat_vertices) = f(lon_bnds, lat_bnds)
        self.assertEqual(lon_vertices[0], 1)
        self.assertEqual(lon_vertices[1], 2)
        self.assertEqual(lon_vertices[2], 3)
        self.assertEqual(lon_vertices[3], 4)
        self.assertEqual(lat_vertices[0], 10)
        self.assertEqual(lat_vertices[1], 12)
        self.assertEqual(lat_vertices[2], 13)

    def test_rectilinear_centroids_to_vertices_01(self):
        lon_centroids = np.array([1, 2, 3, 4])
        lat_centroids = np.array([2, 7, 8])
        f = geogrid.rectilinear_centroids_to_vertices
        lon_vertices, lat_vertices = f(lon_centroids, lat_centroids)
        self.assertEqual(lon_vertices[0], 0.5)
        self.assertEqual(lon_vertices[1], 1.5)
        self.assertEqual(lon_vertices[2], 2.5)
        self.assertEqual(lon_vertices[3], 3.5)
        self.assertEqual(lon_vertices[4], 4.5)
        self.assertEqual(lat_vertices[0], -0.5)
        self.assertEqual(lat_vertices[1], 4.5)
        self.assertEqual(lat_vertices[2], 7.5)
        self.assertEqual(lat_vertices[3], 8.5)

    def test_rectilinear_centroids_to_vertices_02(self):
        lon_centroids = np.array([1, 2, 3, 4])
        lat_centroids = np.array([2, 7, 8])
        f = geogrid.rectilinear_centroids_to_vertices
        lon_vertices, lat_vertices = f(
            lon_centroids, lat_centroids, forced_bounds=True, lower_bound_x=-7,
            upper_bound_x=12, lower_bound_y=-3, upper_bound_y=10)
        self.assertEqual(lon_vertices[0], -7)
        self.assertEqual(lon_vertices[1], 1.5)
        self.assertEqual(lon_vertices[2], 2.5)
        self.assertEqual(lon_vertices[3], 3.5)
        self.assertEqual(lon_vertices[4], 12)
        self.assertEqual(lat_vertices[0], -3)
        self.assertEqual(lat_vertices[1], 4.5)
        self.assertEqual(lat_vertices[2], 7.5)
        self.assertEqual(lat_vertices[3], 10)

    def test_rectilinear_centroids_to_vertices_03(self):
        lon_centroids = np.array([4, 3, 2, 1])
        lat_centroids = np.array([8, 7, 2])
        f = geogrid.rectilinear_centroids_to_vertices
        lon_vertices, lat_vertices = f(
            lon_centroids, lat_centroids, forced_bounds=True, lower_bound_x=-7,
            upper_bound_x=12, lower_bound_y=-3, upper_bound_y=10)
        self.assertEqual(lon_vertices[0], 12)
        self.assertEqual(lon_vertices[1], 3.5)
        self.assertEqual(lon_vertices[2], 2.5)
        self.assertEqual(lon_vertices[3], 1.5)
        self.assertEqual(lon_vertices[4], -7)
        self.assertEqual(lat_vertices[0], 10)
        self.assertEqual(lat_vertices[1], 7.5)
        self.assertEqual(lat_vertices[2], 4.5)
        self.assertEqual(lat_vertices[3], -3)

    def test_rectilinear_centroids_to_vertices_04(self):
        lon_centroids = np.array([1, 2, 3, 4])
        lat_centroids = np.array([2, 7, 8])
        f = geogrid.rectilinear_centroids_to_vertices
        lon_vertices, lat_vertices = f(
            lon_centroids, lat_centroids, limit_bounds=True, lower_bound_x=0.1,
            upper_bound_x=4.2, lower_bound_y=0, upper_bound_y=9)
        self.assertEqual(lon_vertices[0], 0.5)
        self.assertEqual(lon_vertices[1], 1.5)
        self.assertEqual(lon_vertices[2], 2.5)
        self.assertEqual(lon_vertices[3], 3.5)
        self.assertEqual(lon_vertices[4], 4.2)
        self.assertEqual(lat_vertices[0], 0)
        self.assertEqual(lat_vertices[1], 4.5)
        self.assertEqual(lat_vertices[2], 7.5)
        self.assertEqual(lat_vertices[3], 8.5)

    def test_rectilinear_centroids_to_vertices_05(self):
        lon_centroids = np.array([4, 3, 2, 1])
        lat_centroids = np.array([8, 7, 2])
        f = geogrid.rectilinear_centroids_to_vertices
        lon_vertices, lat_vertices = f(
            lon_centroids, lat_centroids, limit_bounds=True, lower_bound_x=0.1,
            upper_bound_x=4.2, lower_bound_y=0, upper_bound_y=9)
        self.assertEqual(lon_vertices[0], 4.2)
        self.assertEqual(lon_vertices[1], 3.5)
        self.assertEqual(lon_vertices[2], 2.5)
        self.assertEqual(lon_vertices[3], 1.5)
        self.assertEqual(lon_vertices[4], 0.5)
        self.assertEqual(lat_vertices[0], 8.5)
        self.assertEqual(lat_vertices[1], 7.5)
        self.assertEqual(lat_vertices[2], 4.5)
        self.assertEqual(lat_vertices[3], 0)

suite = unittest.TestLoader().loadTestsFromTestCase(TestGeogrid)

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite)
