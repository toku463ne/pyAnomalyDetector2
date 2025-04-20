"""
unit tests for sample_getter.py
"""
import unittest, os
import time

import __init__

from data_getter.csv_getter import CsvGetter


class TestCsvGetter(unittest.TestCase):
    def test_csv_getter(self):
        data_source = {
            'type': 'csv',
            'data_dir': 'testdata/csv/20250214_1100'
        }
        csv_getter = CsvGetter(data_source)
        self.assertIsNotNone(csv_getter)

        # check connection
        self.assertTrue(csv_getter.check_conn())

        endep = 1739505557
        trend_startep = endep - 3600 * 12
        
        """
        'app/iim' = []
        'app/sim' = [267903]
        'app/cal' = [59888, 141917]
        'app/bcs' = [255218]
        'hw/nw' = [93281, 110309, 94003]
        'hw/pc' = [217822, 217823, 217825, 232310, 236160]
        """

        # get trends data
        itemIds = [59888,  93281,  94003, 110309, 141917, 217822, 217823, 217825,
            232310, 236160, 255218, 267903, 270747, 270750, 270784, 270790,
            270793, 270797]
        df = csv_getter.get_trends_data(trend_startep, endep, itemIds)
        self.assertEqual(len(df["itemid"].unique()), 18)

        item_59888 = df[df['itemid'] == 59888]
        self.assertGreaterEqual(item_59888['clock'].min(), trend_startep)
        self.assertLessEqual(item_59888['clock'].max(), endep)
        self.assertGreater(len(item_59888), 0)

        history_startep = endep - 3600 * 3
        # get history data
        df = csv_getter.get_history_data(history_startep, endep, itemIds)
        self.assertEqual(len(df["itemid"].unique()), 18)

        item_59888 = df[df['itemid'] == 59888]
        self.assertGreaterEqual(item_59888['clock'].min(), history_startep)
        self.assertLessEqual(item_59888['clock'].max(), endep)
        self.assertGreater(len(item_59888), 0)

        # classify itemIds by groups
        group_names = ['app/iim', 'app/sim', 'app/cal', 'app/bcs', 'hw/nw', 'hw/pc']
        groups = csv_getter.classify_by_groups(itemIds, group_names)
        self.assertEqual(len(groups), 6)
        self.assertEqual(len(groups['app/iim']), 0)
        self.assertEqual(len(groups['app/sim']), 1)
        self.assertEqual(len(groups['app/cal']), 1)
        self.assertEqual(len(groups['app/bcs']), 1)
        self.assertEqual(len(groups['hw/nw']), 3)
        self.assertEqual(len(groups['hw/pc']), 5)

        
if __name__ == '__main__':
    unittest.main()