import unittest

import __init__
from models.models_set import ModelsSet
import utils.config_loader as config_loader
import detect_anomalies
import trends_stats


class TestDetector(unittest.TestCase):
    
    def test_history_stats(self):
        name = 'test_detect3'
        config = config_loader.conf
        config['data_sources'] = {}
        config['data_sources'][name] = {
                'data_dir': "testdata/csv/20250214_1100",
                'type': 'csv'
            }
        ms = ModelsSet(name)
        ms.initialize()
        
        
        itemIds = [59888, 93281, 94003, 110309, 141917, 217822, 236160, 217825, 270793, 270797, 217823]

        endep = 1739505598 - 3600*24*3
        trends_stats.update_stats(config, endep, 0, itemIds=itemIds, initialize=True)
        
                
        # second data load
        endep = 1739505598 - 600*18
        itemIds = detect_anomalies.run(config, endep, itemIds, detection_stages=[detect_anomalies.STAGE_DETECT3])

                

if __name__ == '__main__':
    unittest.main()

    # streamlit run /home/ubuntu/git/pyAnomalyDetector2/tests/test_detector_detect2.py

