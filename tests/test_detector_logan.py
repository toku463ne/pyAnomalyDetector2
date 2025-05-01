import unittest
import os, time

import __init__
from models.models_set import ModelsSet
import utils.config_loader as config_loader
from data_processing.detector import Detector
from utils import normalizer
import trends_stats
import detect_anomalies


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
        name = 'test_detector_logan'
        ms = ModelsSet(name)
        ms.initialize()
        config = config_loader.conf
        config['data_sources'] = {}
        config['data_sources'][name] = {
            'base_url': 'http://localhost:8002/',
            'type': 'logan',
            'groups': {
                'proxy': {
                    1: 'SOPHOS-01',
                    2: 'pfsense67051_openvpn'
                },
                'firewall': {
                    3: 'IMTFW001',
                    4: 'NFPFW003',
                },
            },
            'minimal_group_size': 1000
            }
        config['history_interval'] = 3600
        config['detect1_lambda_threshold'] = 2.0

        os.system('cd testdata/loganal && python3 -m http.server 8002 &')
        time.sleep(1)
        
        itemIds = []
        endep = 1745913600
        trends_stats.update_stats(config, endep, 0, itemIds=itemIds, initialize=True)
        itemIds = detect_anomalies.run(config, endep, itemIds)


if __name__ == '__main__':
    unittest.main()