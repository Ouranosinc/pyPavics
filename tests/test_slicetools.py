import unittest

import numpy as np
import numpy.ma as ma

import slicetools as st


class TestSliceTools(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_divide_slice(self):
        divided_slices = st.divide_slice(slice(0, 10), 2)
        self.assertEqual(divided_slices[0], slice(0, 5, 1))
        self.assertEqual(divided_slices[1], slice(5, 10, 1))

        divided_slices = st.divide_slice(slice(0, 10, 2), 2)
        self.assertEqual(divided_slices[0], slice(0, 4, 2))
        self.assertEqual(divided_slices[1], slice(4, 10, 2))

suite = unittest.TestLoader().loadTestsFromTestCase(TestSliceTools)

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite)
