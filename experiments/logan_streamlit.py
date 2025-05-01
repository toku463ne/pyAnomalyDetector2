import os, time

import __init__
from data_getter.logan_getter import LoganGetter
from models.models_set import ModelsSet
import utils.config_loader as config_loader
import trends_stats

name = 'test_logan'
data_source = {
    'base_url': 'http://localhost:8100/',
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
    'minimal_group_size': 10000
}
config_loader.conf['data_sources'] = {}
config_loader.conf['data_sources'][name] = data_source

os.system('cd testdata/loganal && python3 -m http.server 8100 &')
time.sleep(1)

logan_getter = LoganGetter(data_source)
details = logan_getter.get_items_details()


ms = ModelsSet(name)
ms.initialize()
endep = 1745913600
trends_stats.update_stats(config_loader.conf, endep, initialize=True)

ts = ms.trends_stats.read_stats()

# merge ts and details
ts = details.merge(ts, on='itemid', how='left')

print(ts)

# ["itemid", "created", "group_name", "hostid", "clusterid", "host_name", "item_name", "trend_mean", "trend_std"]




