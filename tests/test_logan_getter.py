import unittest, os
import time

import __init__

from data_getter.logan_getter import LoganGetter

class TestLoganGetter(unittest.TestCase):
    
    def test_logan_getter(self):
        # csv data is in testdata/loganal/sophos
        # start a local server to serve the csv files
        os.system('cd testdata/loganal && python3 -m http.server 8000 &')
        time.sleep(1)
        data_source = {
            'base_url': 'http://localhost:8000/',
            'group_names': {
                'hw/nw': {
                    'host_names': ['sophos']
                }
            },
            'trends_interval': 3600 * 3,
            'minimal_group_size': 100
        }
        logan_getter = LoganGetter(data_source)
        self.assertIsNotNone(logan_getter)
        self.assertTrue(logan_getter.check_conn())
        
        # get itemIds
        itemIds = logan_getter.get_itemIds()
        self.assertGreater(len(itemIds), 0)

        # Fri Mar 21 03:00:00 JST 2025
        start_time_epoch = int(time.mktime(time.strptime('Fri Mar 21 09:00:00 2025', '%a %b %d %H:%M:%S %Y')))
        # maximum = Fri Mar 21 20:00:00 JST 2025
        end_time_epoch = int(time.mktime(time.strptime('Fri Mar 21 20:00:00 2025', '%a %b %d %H:%M:%S %Y')))
        

        history_data = logan_getter.get_history_data(start_time_epoch, end_time_epoch, itemIds)
        self.assertGreater(len(history_data), 0)

        history_data = logan_getter.get_history_data(start_time_epoch, end_time_epoch, itemIds=[1742496227000000001])
        self.assertEqual(len(history_data), 12)


        trends_data = logan_getter.get_trends_data(start_time_epoch, end_time_epoch, itemIds)
        self.assertGreater(len(trends_data), 0) 

        trends_data = logan_getter.get_trends_data(start_time_epoch, end_time_epoch, itemIds=[1742496227000000001])
        self.assertEqual(len(trends_data), 1) 


        trends_full_data = logan_getter.get_trends_full_data(start_time_epoch, end_time_epoch, itemIds)
        self.assertGreater(len(trends_full_data), 0)

        trends_full_data = logan_getter.get_trends_full_data(start_time_epoch, end_time_epoch, itemIds=[1742496227000000001])
        self.assertEqual(len(trends_full_data), 1)

        os.system('pkill -f http.server')



if __name__ == '__main__':
    unittest.main()