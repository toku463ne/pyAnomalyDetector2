"""
unit tests for utils/formatter.py
"""
import unittest
import pandas as pd

import __init__

from utils.normalizer import *
#from utils.data_generator import *

class TestFormatter(unittest.TestCase):
    # test fit_to_base_clocks
    def test_fit_to_base_clocks(self):
        clocks = [2, 4, 5, 7, 9]
        values = [1, 2, 3, 4, 5]
        base_clocks = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        expected_values  = [1, 1, 2, 2, 3, 4, 4, 5, 5, 5]
        new_values = fit_to_base_clocks(base_clocks, clocks, values)
        self.assertEqual(new_values, expected_values)

        clocks = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        base_clocks = [2, 4, 5, 7, 9]
        expected_values = [1.5,3.5,5,6.5,9.25]
        new_values = fit_to_base_clocks(base_clocks, clocks, values)
        self.assertEqual(new_values, expected_values)

        
        
        



if __name__ == '__main__':
    unittest.main()