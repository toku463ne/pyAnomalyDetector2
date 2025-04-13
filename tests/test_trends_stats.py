import unittest

import __init__
import trends_stats
from models.models_set import ModelsSet
import utils.config_loader as config_loader
import data_getter

name = 'test_trends_stats'
class TestTrendsStats(unittest.TestCase):
    def run_update_test(self, n_expected_items, config, endep, itemIds, initialize, exluded_itemIds=[]):
        dg = data_getter.get_data_getter(config['data_sources'][name])
        self.assertIsNotNone(dg)

        ms = ModelsSet(name)
        old_startep = ms.trends_updates.get_startep()
        old_endep = ms.trends_updates.get_endep()
        trends_stats.update_stats(config, endep, 0, itemIds=itemIds, initialize=initialize)
        
        stats_df = ms.trends_stats.read_stats()
        self.assertEqual(len(stats_df), n_expected_items)

        startep = endep - config['trends_interval'] * config['trends_retention']        
        new_startep = ms.trends_updates.get_startep()
        new_endep = ms.trends_updates.get_endep()
        self.assertLessEqual(new_startep, startep)
        self.assertLessEqual(new_endep, endep)
        self.assertGreater(new_startep, old_startep)
        self.assertGreater(new_endep, old_endep)

        df_dg = dg.get_trends_data(startep=startep-1, endep=endep-1, itemIds=itemIds)
        for itemId in itemIds:
            if itemId in exluded_itemIds:
                continue
            try:
                row = stats_df[stats_df['itemid'] == itemId].iloc[0]
                df_item = df_dg[df_dg['itemid'] == itemId]
                item_mean = df_item['value'].mean()
                item_std = df_item['value'].std()
                item_sum = df_item['value'].sum()
                item_cnt = len(df_item)
                #item_sqr = (df_item['value'] ** 2).sum()
                self.assertEqual(row['cnt'], item_cnt)
                self.assertAlmostEqual(row['sum'], item_sum, places=1)
                #self.assertAlmostEqual(row['sqr_sum'], item_sqr, places=1)
                self.assertAlmostEqual(row['mean'], item_mean, places=2)
                self.assertAlmostEqual(row['std'], item_std, places=1)
                self.assertGreater(abs(row['mean']), 0)
                self.assertGreaterEqual(row['cnt'], 0)
                self.assertGreater(abs(row['sum']), 0)
                self.assertGreater(row['sqr_sum'], 0)
            except AssertionError as e:
                print(f"Assertion failed for itemId {itemId}: {e}")
                raise
            except Exception as e:
                print(f"Unexpected error for itemId {itemId}: {e}")
                raise

            
            
        

    def test_stats(self):
        name = 'test_trends_stats'
        ms = ModelsSet(name)
        ms.initialize()
        config = config_loader.conf

        config['data_sources'] = {}
        config['data_sources'][name] = {
                'data_dir': "testdata/csv/20250214_1100",
                'type': 'csv'
            }
        
        dg = data_getter.get_data_getter(config['data_sources'][name])
        self.assertIsNotNone(dg)

        itemIds = [59888,  93281,  94003, 110309, 141917, 217822]
        
        # first data load
        endep = 1739505557 - 3600*24*2
        self.run_update_test(6, config, endep, itemIds, True)
        
        
        # second data load
        endep = 1739505557 - 3600*24
        self.run_update_test(6, config, endep, itemIds, False)

        # 3rd data load: remove 1 item and add 1 item
        itemIds = [93281,  94003, 110309, 141917, 217822, 217823]
        endep = 1739505557
        self.run_update_test(7, config, endep, itemIds, False)
        

if __name__ == '__main__':
    unittest.main()