import unittest

import __init__
from models.models_set import ModelsSet


class TestTrendsStatsModel(unittest.TestCase):
    def test_stats(self):
        name = 'test_trends_stats_model'
        
        ms = ModelsSet(name)
        tsm = ms.trends_stats
        
        tsm.initialize()
        
        itemIds = [59888,  93281]
        
        self.assertEqual(tsm.count(), 0)
        tsm.upsert_stats(itemid=59888, sum=0.0, sqr_sum=0.0, cnt=0, mean=0.0, std=0.0)
        tsm.upsert_stats(itemid=93281, sum=1.0, sqr_sum=1.0, cnt=1, mean=1.0, std=0.0)

        self.assertEqual(tsm.count(), 2)
        df = tsm.read_stats(itemids=itemIds)
        self.assertEqual(len(df), 2)
        self.assertEqual(df['itemid'].tolist(), itemIds)

        # test read_stats with empty itemids
        df = tsm.read_stats(itemids=[])
        self.assertEqual(len(df), 2)
        self.assertEqual(df['itemid'].tolist(), itemIds)

        df = tsm.read_stats(itemids=[93281])
        
        self.assertEqual(len(df), 1)
        self.assertEqual(df['itemid'].tolist(), [93281])
        

if __name__ == '__main__':
    unittest.main()