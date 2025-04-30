import unittest

import __init__
from models.models_set import ModelsSet
import utils.config_loader as config_loader
from data_processing.detector import Detector
from utils import normalizer


class TestDetector(unittest.TestCase):
    def run_update_test(self, name, config, endep, itemIds, itemId_to_check):
        data_source = config['data_sources'][name]
        history_interval = config['history_interval']
        anomaly_keep_secs = config['anomaly_keep_secs']

        ms = ModelsSet(name)
        
        # function to test
        d = Detector(name, data_source, itemIds)
        d.update_history(endep, itemIds)
        
        df = ms.history.get_data(itemIds)
        self.assertEqual(len(df["itemid"].unique()), len(itemIds))

        min_clock = df["clock"].min()
        max_clock = df["clock"].max()

        startep = endep - anomaly_keep_secs
        self.assertEqual(min_clock, startep - startep % history_interval) 
        self.assertEqual(max_clock, endep - endep % history_interval)

        base_clocks = normalizer.get_base_clocks(startep, endep, history_interval)
        self.assertEqual(df[df["itemid"] == itemId_to_check]["clock"].count(), len(base_clocks))
        

            

    def test_update_history(self):
        name = 'test_update_history'
        ms = ModelsSet(name)
        ms.initialize()
        config = config_loader.conf

        config['data_sources'] = {}
        config['data_sources'][name] = {
                'data_dir': "testdata/csv/20250214_1100",
                'type': 'csv'
            }
        config['history_interval'] = 600
        
        itemIds = [59888,  93281,  94003, 110309, 141917, 217822]
        endep = 1739505598 - 1800
        self.run_update_test(name, config, endep, itemIds, 59888)

        endep = 1739505598
        self.run_update_test(name, config, endep, itemIds, 59888)
        


if __name__ == '__main__':
    unittest.main()