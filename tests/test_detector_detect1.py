import unittest

import __init__
from models.models_set import ModelsSet
import utils.config_loader as config_loader
from data_processing.detector import Detector
import trends_stats

class TestDetector(unittest.TestCase):
    def run_update_test(self, name, n_expected_items, config, endep, itemIds, initialize):
        data_source = config['data_sources'][name]

        ms = ModelsSet(name)
        old_startep = ms.history_updates.get_startep()
        old_endep = ms.history_updates.get_endep()

        # function to test
        d = Detector(name, data_source, itemIds)
        d.update_history_stats(endep, initialize=initialize)

        stats_df = ms.history_stats.read_stats()
        self.assertEqual(len(stats_df), n_expected_items)
        startep = endep - config['history_interval'] * config['history_retention']        
        new_startep = ms.history_updates.get_startep()
        new_endep = ms.history_updates.get_endep()
        self.assertLessEqual(new_startep, startep)
        self.assertLessEqual(new_endep, endep)
        self.assertGreater(new_startep, old_startep)
        self.assertGreater(new_endep, old_endep)

            

    def test_history_stats(self):
        name = 'test_detect1'
        ms = ModelsSet(name)
        ms.initialize()
        config = config_loader.conf

        config['data_sources'] = {}
        config['data_sources'][name] = {
                'data_dir': "testdata/csv/20250214_1100",
                'type': 'csv'
            }
        data_source = config['data_sources'][name]
        
        itemIds = [59888, 93281, 94003, 110309, 141917, 217822, 236160, 217825, 270793, 270797, 217823]

        endep = 1739505598 - 3600*24*3
        trends_stats.update_stats(config, endep, 0, itemIds=itemIds, initialize=True)
        
                
        # second data load
        endep = 1739505598 - 600*18
        
        d = Detector(name, data_source, itemIds)
        d.update_history_stats(endep)

        anomaly_itemIds = d.detect1()
        self.assertIsNotNone(anomaly_itemIds)
        self.assertGreater(len(anomaly_itemIds), 0)



if __name__ == '__main__':
    unittest.main()