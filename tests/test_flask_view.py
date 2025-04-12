import __init__
import unittest
from flask import Flask

import multiprocessing
import requests
import time

from views.flask_view import FlaskView
from models.models_set import ModelsSet
import pandas as pd

history_end = 1739505557
trend_start = history_end - 3600 * 12
history_start = history_end - 3600 * 3
itemIds = [59888,  93281,  94003, 110309, 141917, 217822, 217823, 217825,
            232310, 236160, 255218, 267903, 270747, 270750, 270784, 270790,
            270793, 270797]

class TestFlaskView(unittest.TestCase):
    def insert_anomalies(self, data_source_name):
        ms = ModelsSet(data_source_name)
        ms.anomalies.initialize()

        data = [
            (59888, 1739500000, 1, 1, 'app/cal', 'host1', 'item1'),
            (141917, 1739500100, 1, 1, 'app/cal', 'host2', 'item2'),
            (255218, 1739500200, 1, 1, 'app/bcs', 'host3', 'item3'),
            (267903, 1739500300, 1, 1, 'app/sim', 'host4', 'item4'),
            (93281, 1739500400, 1, 1, 'hw/nw', 'host5', 'item5'),
            (110309, 1739500500, 1, 1, 'hw/nw', 'host6', 'item6'),
            (94003, 1739500600, 1, 1, 'hw/nw', 'host7', 'item7'),
            (217822, 1739500700, 1, 1, 'hw/pc', 'host8', 'item8'),
            (217823, 1739500800, 1, 1, 'hw/pc', 'host9', 'item9'),
            (217825, 1739500900, 1, 1, 'hw/pc', 'host10', 'item10'),
            (232310, 1739501000, 1, 1, 'hw/pc', 'host11', 'item11'),
            (236160, 1739501100, 1, 1, 'hw/pc', 'host12', 'item12'),
        ]

        columns = ['itemid', 'created', 'hostid', 'clusterid', 'group_name', 'host_name', 'item_name']
        df = pd.DataFrame(data, columns=columns)

        ms.anomalies.insert_data(df)
        self.assertEqual(ms.anomalies.count(), 12)

    def get_conf(self, port):
        return {
            "host": "localhost",
            "port": port,
            "debug": True,
            "type": "flask",
            "layout": {
                "chart_width": 400,
                "chart_height": 400,
                "max_vertical_charts": 4,
                "max_horizontal_charts": 4
            },
            "trend_start": trend_start,
            "history_start": history_start,
            "history_end": history_end,
            "itemids": itemIds
        }
    
    def get_datasources(self, test_name):
        data_sources = {}
        data_sources[test_name] = {
                "type": "csv",
                "data_dir": "testdata/csv/20250214_1100"
            }
        return data_sources

    def run_app(self, test_name, port):
        conf = self.get_conf(port)
        data_sources = self.get_datasources(test_name)
        fv = FlaskView(conf, data_sources)
        fv.run()

    def test_generate_charts(self):
        port = 80
        conf = self.get_conf(port)
        data_sources = self.get_datasources("test_generate_charts")
        self.insert_anomalies(list(data_sources.keys())[0])
        
        fv = FlaskView(conf, data_sources)
        charts_html = fv._generate_charts()
        self.assertEqual(len(charts_html), 6)

    def __test_chart(self):
        port = 5101
        test_name = "test_flask_chart"
        process = multiprocessing.Process(target=self.run_app, args=(test_name, port))
        process.start()
        self.addCleanup(process.terminate)
        
        # Wait for the Flask app to start
        time.sleep(1)

        # Test connection to /charts endpoint
        response = requests.get(f"http://localhost:{port}/charts")
        
        self.assertEqual(response.status_code, 200)

    def __test_status(self):
        port = 5100
        test_name = "test_flask_status"
        process = multiprocessing.Process(target=self.run_app, args=(test_name, port))
        process.start()
        self.addCleanup(process.terminate)
        
        # Wait for the Flask app to start
        time.sleep(1)

        # Test connection to /status endpoint
        response = requests.get(f"http://localhost:{port}/status")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "OK"})

        # Stop FlaskView
        process.terminate()
        process.join()

if __name__ == "__main__":
    unittest.main()