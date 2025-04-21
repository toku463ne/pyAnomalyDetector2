"""
unit tests for zabbix_dashboard.py
"""
import unittest
import pandas as pd

import __init__
from models.models_set import ModelsSet
import utils.config_loader as config_loader
import reporter

class TestReporter(unittest.TestCase):  
    def test_reporter(self):
        config = config_loader.conf
        
        name = "test_reporter"
        config["data_sources"] = {
            "test_reporter": {
                "type": "csv",
                "data_dir": "testdata/csv/20250214_1100"
            }
        }

        epochs = range(1739498400 - 3600*24*3 - 3600, 1739497800, 3600)
        itemIds = [111, 312, 333, 334]
        hosts = ["host1", "host1", "host3", "host3"]
        hostIds = [1, 1, 3, 3]
        clusterIds = [1, 3, 3, 3]
        groupNames = ["group1", "group1", "group3", "group3"]
        itemNames = ["item1", "item2", "item3", "item4"]
        trendMeans = [0.1, 0.2, 0.3, 0.4]
        trendStds = [0.1, 0.2, 0.3, 0.4]

        df = pd.DataFrame(columns=["itemid", "created", 
                                   "group_name", "hostid", "clusterid", "host_name", "item_name",
                                   "trend_mean", "trend_std"], dtype=object)
        for epoch in epochs:
            created = [epoch]*4
            df = pd.concat([df, pd.DataFrame({
                "itemid": itemIds,
                "created": created,
                "group_name": groupNames,
                "hostid": hostIds,
                "clusterid": clusterIds,
                "host_name": hosts,
                "item_name": itemNames,
                "trend_mean": trendMeans,
                "trend_std": trendStds
            })], ignore_index=True)

        ms = ModelsSet(name)
        ms.anomalies.truncate()
        ms.anomalies.insert_data(df)
        result = reporter.report(config, epochs[-1])
        
        self.assertEqual(result, {
            "test_reporter": {
                3: {
                    "group1": {
                        "host1": ["312: item2"]
                    },
                    "group3": {
                        "host3": ["333: item3"]
                    }
                }
            }
        })


if __name__ == '__main__':
    unittest.main()