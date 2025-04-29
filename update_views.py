"""
To update 3rd party views like Zabbix dashboard
"""
target_views = ["zabbix_dashboard"]
from typing import Dict

import utils.config_loader as config_loader
import views

def update(conf: Dict) :
    view_sources = conf.get('view_sources', [])
    for view_source_name in view_sources:
        if view_source_name not in target_views:
            continue
        view_source = view_sources[view_source_name]
        v = views.get_view(view_source)
        v.update()

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

    