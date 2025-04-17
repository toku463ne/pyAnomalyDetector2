import unittest
import numpy as np

import __init__
import utils


class TestUtils(unittest.TestCase):
    def test_get_digits_range(self):
        def test_case(a, mask_len, expected):
            result = utils.get_float_format(a, mask_len)
            self.assertEqual(result, expected)

        test_case(np.array([0.00112345, 0.001]), 4, ".7g")
        test_case(np.array([0.00198761, 0.001]), 4, ".7g")
        test_case(np.array([1.1234, 1]), 4, ".5g")
        test_case(np.array([111234.56, 110000]), 4, ".6g")
        test_case(np.array([1111.1111 + 98.76, 1111.1111]), 4, ".6g")
        


if __name__ == '__main__':
    unittest.main()