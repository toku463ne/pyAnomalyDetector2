import __init__
import unittest
import os
import pandas as pd

from models.anomalies import AnomaliesModel
from models.models_set import ModelsSet


class TestAnomaliesModel(unittest.TestCase):
    def test_anomalies_model(self):
        ms = ModelsSet("test_anomalies_model")
        anomalies = ms.anomalies
        self.assertTrue(anomalies.check_conn())
        anomalies.initialize()
        self.assertEqual(anomalies.count(), 0)
        
        # fields = "itemid", "created", "group_name", "hostid", "clusterid", "host_name", "item_name", "trend_mean", "trend_std"
        # table_name = "anomalies"
        # primary keys = itemid, created, group_name
        
        # Test insert_data
        data = pd.DataFrame({
            "itemid": [1, 2],
            "created": [1234567890, 1234567891],
            "group_name": ["group1", "group2"],
            "hostid": [101, 102],
            "clusterid": [201, 202],
            "host_name": ["host1", "host2"],
            "item_name": ["item1", "item2"],
            "trend_mean": [0.1, 0.2],
            "trend_std": [0.01, 0.02]
        })
        anomalies.insert_data(data)
        self.assertEqual(anomalies.count(), 2)

        # Test get_data
        retrieved_data = anomalies.get_data()
        self.assertEqual(len(retrieved_data), 2)

        # Test get_itemids
        itemids = anomalies.get_itemids()
        self.assertEqual(len(itemids), 2)


        last_updated = anomalies.get_last_updated()
        self.assertGreater(last_updated, 0)



if __name__ == "__main__":
    unittest.main()