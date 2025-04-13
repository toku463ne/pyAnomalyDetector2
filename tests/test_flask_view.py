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
trend_start = history_end - 3600 * 24 * 1
history_start = history_end - 3600 * 3
itemIds = [59888,  93281,  94003, 110309, 141917, 217822, 217823, 217825,
            232310, 236160, 255218, 267903, 270747, 270750, 270784, 270790,
            270793, 270797]
#itemIds = [59888]

class TestFlaskView(unittest.TestCase):
    def insert_anomalies(self, data_source_name):
        ms = ModelsSet(data_source_name)
        ms.anomalies.initialize()

        data = [
            (59888, 1739500000, 10769, 1, 'app/cal', 'DB135', 'vfs.fs.inode[/u01,pfree]', 0,0),
            (93281, 1739500400, 10967, 1, 'hw/nw', 'SW007', 'net.if.out[ifOutOctets.25]', 0,0),
            (94003, 1739500600, 10971, 1, 'hw/nw', 'SW010', 'net.if.duplex[dot3StatsDuplexStatus.25]', 0,0),
            (110309, 1739500500, 10971, 1, 'hw/nw', 'SW010', 'net.if.duplex[dot3StatsDuplexStatus.24]', 0,0),
            (141917, 1739500100, 11299, 1, 'app/cal', 'db022', 'mysql.innodb_row_lock_time', 0,0),
            (217822, 1739500700, 11379, 1, 'hw/pc', 'vm1', 'vmware.vm.vfs.fs.size[{$VMWARE.URL},{$VMWARE.VM.UUID},C:\\,free]', 0,0),
            (217823, 1739500800, 11379, 1, 'hw/pc', 'vm1', 'vmware.vm.vfs.fs.size[{$VMWARE.URL},{$VMWARE.VM.UUID},C:\\,pfree]', 0,0),
            (217825, 1739500900, 11379, 1, 'hw/pc', 'vm1', 'vmware.vm.vfs.fs.size[{$VMWARE.URL},{$VMWARE.VM.UUID},C:\\,used]', 0,0),
            (232310, 1739501000, 11624, 1, 'hw/pc', 'VMSRV079', 'vmware.hv.memory.used[{$VMWARE.URL},{$VMWARE.HV.UUID}]', 0,0),
            (236160, 1739501100, 11679, 1, 'hw/pc', 'vm2', 'vmware.vm.vfs.fs.size[{$VMWARE.URL},{$VMWARE.VM.UUID},D:\\,pfree]', 0,0),
            (255218, 1739500200, 11789, 1, 'app/bcs', 'IPX062', 'vm.memory.cached[memCached.0]', 0,0),
            (267903, 1739500300, 11906, 1, 'app/imt', 'DOCKER083', 'system.swap.size[,pfree]', 0,0),
            (270747, 1739501200, 11955, 1, 'app/imt', 'PC-02', 'net.if.out[{HOST.CONN},bytes]', 0,0),
            (270750, 1739501200, 11955, 1, 'app/imt', 'PC-02', 'vm.memory.size[used]', 0,0),
            (270784, 1739501200, 11955, 1, 'app/imt', 'PC-02', 'vfs.fs.size[D:,pused]', 0,0),
            (270790, 1739501200, 11955, 1, 'app/imt', 'PC-02', 'vfs.fs.size[D:,used]', 0,0),
            (270793, 1739501200, 11956, 1, 'app/imt', 'PC-01', 'vfs.fs.size[D:,pused]', 0,0),
            (270797, 1739501200, 11956, 1, 'app/imt', 'PC-01', 'vfs.fs.size[D:,used]', 0,0),
        ]

        columns = ['itemid', 'created', 'hostid', 'clusterid', 'group_name', 'host_name', 'item_name', 'trend_mean', 'trend_std']
        df = pd.DataFrame(data, columns=columns)

        ms.anomalies.insert_data(df)
        self.assertEqual(ms.anomalies.count(), 18)

    def get_conf(self, port):
        return {
            "host": "localhost",
            "port": port,
            "debug": True,
            "type": "flask",
            "layout": {
                "chart_width": 400,
                "chart_height": 300,
                "max_vertical_charts": 4,
                "max_horizontal_charts": 4,
                "chart_categories": ["bygroup"]
            },
            "trend_start": trend_start,
            "history_start": history_start,
            "history_end": history_end,
            "itemids": itemIds,
            "tmp_dir": "/mnt/c/tmp",
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
        charts_html = fv._generate_charts("bygroup")
        self.assertIn("DB135", charts_html)


    # test manually and check the browser
    def disabled_test_chart(self):
        port = 5101
        test_name = "test_flask_chart"
        data_sources = self.get_datasources(test_name)
        self.insert_anomalies(list(data_sources.keys())[0])

        process = multiprocessing.Process(target=self.run_app, args=(test_name, port))
        process.start()
        self.addCleanup(process.terminate)
        
        # Wait for the Flask app to start
        time.sleep(1)

        # Test connection to /charts endpoint
        response = requests.get(f"http://localhost:{port}/charts")
        
        self.assertEqual(response.status_code, 200)



    def test_status(self):
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