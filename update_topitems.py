from typing import Dict, List, Tuple
import logging

import utils.config_loader as config_loader
import data_getter
from data_processing.detector import Detector
import classifiers.dbscan as dbscan
from models.models_set import ModelsSet

def log(msg, level=logging.INFO):
    msg = f"[update_topitems.py] {msg}"
    print(msg)
    logging.log(level, msg)


def run(conf: Dict, endep: int = 0, 
        item_names: List[str] = None, 
        host_names: List[str] = None, 
        group_names: List[str] = None,
        itemIds: List[int] = None,
        max_itemIds = 0):
    
    log("starting")

    if item_names is None:
        item_names = conf.get('item_names', [])
    if host_names is None:
        host_names = conf.get('host_names', [])
    if group_names is None:
        group_names = conf.get('group_names', [])
    if itemIds is None:
        itemIds = conf.get('itemIds', [])

    data_sources = conf['data_sources']
    for data_source_name in data_sources:
        data_source = data_sources[data_source_name]
        data_source['name'] = data_source_name
        dg = data_getter.get_data_getter(data_source)
        itemIds = dg.get_itemIds(item_names=item_names, 
                                 host_names=host_names, 
                                 group_names=group_names,
                                 itemIds=itemIds,
                                 max_itemIds=max_itemIds)
        d = Detector(data_source_name, data_source, itemIds)
        d.initialize_data()
        group_map = {}
        if len(itemIds) > 0 and len(group_names) > 0:
            group_map = dg.get_group_map(itemIds, group_names)

        d.ms.topitems.initialize()
        d.update_topitems(endep, itemIds, group_map=group_map, top_n=data_source.get('top_n', 0))

def classify_charts(endep: int):
    log("starting classification")
    # classify anomaly charts
    classified_itemIds = []
    conf = config_loader.conf
    data_sources = conf['data_sources']
    for data_source_name in data_sources:
        d = Detector(data_source_name, data_sources[data_source_name])
        topitems = ModelsSet(data_source_name).topitems
        top_itemIds = topitems.get_itemids()
        if len(top_itemIds) == 0:
            continue
        d.update_history(endep, top_itemIds)
        classified_itemIds.extend(top_itemIds)
    if len(classified_itemIds) > 1:
        log("classifying charts")
        clusters, _, _ = dbscan.classify_charts(conf, classified_itemIds, endep=endep)
        ModelsSet(data_source_name).topitems.update_clusterid(clusters)
    else:
        log("no data to classify")

    log("completed")

if __name__ == "__main__":
    # read arguments
    import argparse
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-c', '--config', type=str, help='config yaml file')
    parser.add_argument('--end', type=int, default=0, help='End epoch.')
    parser.add_argument('--itemids', type=int, nargs='+', help='itemids')
    parser.add_argument('--items', type=str, nargs='+', help='item names')
    parser.add_argument('--hosts', type=str, nargs='+', help='host names')
    parser.add_argument('--groups', type=str, nargs='+', help='host group names')
    args = parser.parse_args()

    # suppress python warnings
    import warnings
    warnings.filterwarnings("ignore")
    config = config_loader.load_config(args.config)

    run(config, args.end, 
        item_names=args.items, 
        host_names=args.hosts, 
        group_names=args.groups,
        itemIds=args.itemids
    )
    classify_charts(args.end)