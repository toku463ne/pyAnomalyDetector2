import unittest
import os, time

import __init__
from models.models_set import ModelsSet
import utils.config_loader as config_loader
import trends_stats
import update_topitems


class TestDetector(unittest.TestCase):
    
    def test_update_history(self):
        name = 'test_topitems_logan'
        ms = ModelsSet(name)
        ms.initialize()
        config = config_loader.conf
        config['data_sources'] = {}
        config['data_sources'][name] = {
            'base_url': 'http://localhost:8003/',
            'type': 'logan',
            'data_dir': 'tmp/anomdec/test_topitems_logan',
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
            'minimal_group_size': 1000,
            'top_n': 10,
            }
        config['history_interval'] = 3600
        config['detect1_lambda_threshold'] = 2.0

        os.system('cd testdata/loganal && python3 -m http.server 8003 &')
        time.sleep(1)
        
        itemIds = []
        endep = 1745913600
        trends_stats.update_stats(config, endep, 0, itemIds=itemIds, initialize=True)
        update_topitems.run(config, endep, itemIds)
        
        df = ms.topitems.get_data()
        self.assertEqual(len(df), 10)


if __name__ == '__main__':
    unittest.main()