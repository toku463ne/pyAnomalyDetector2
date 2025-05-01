"""
To update 3rd party views like Zabbix dashboard
"""
target_views = ["zabbix_dashboard"]
from typing import Dict

import utils.config_loader as config_loader
from models.models_set import ModelsSet
import views

def update(conf: Dict) :
    view_sources = conf.get('view_sources', [])
    for view_source_name in view_sources:
        view_source = view_sources[view_source_name]
        view_source["name"] = view_source_name
        if view_source["type"] not in target_views:
            continue
        ms = ModelsSet(view_source["data_source_name"])
        df = ms.anomalies.get_data()
        v = views.get_view(view_source)
        v.update(df)
        v.update_cluster(df)

if __name__ == "__main__":
    # read arguments
    import argparse
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-c', '--config', type=str, help='config yaml file')
    
    args = parser.parse_args()
    config_file = args.config
    config_loader.load_config(config_file)
    conf = config_loader.conf
    update(conf)

    