from typing import Dict, List, Tuple
import logging


import utils.config_loader as config_loader
import data_getter
from data_processing.detector import Detector
import classifiers.dbscan as dbscan
from models.models_set import ModelsSet

STAGE_DETECT1 = 1
STAGE_DETECT2 = 2
STAGE_DETECT3 = 3
DETECTION_STAGES = [
    STAGE_DETECT1,
    STAGE_DETECT2,
    STAGE_DETECT3,
]

def log(msg, level=logging.INFO):
    msg = f"[detect_anomalies.py] {msg}"
    print(msg)
    logging.log(level, msg)


def init(conf: Dict):
    data_sources = conf['data_sources']
    for data_source_name in data_sources:
        data_source = data_sources[data_source_name]
        log(f"processing data source: {data_source_name}")

        d = Detector(data_source_name, data_source)
        d.initialize_data()
        


def run(conf: Dict, endep: int = 0, 
        item_names: List[str] = None, 
        host_names: List[str] = None, 
        group_names: List[str] = None,
        itemIds: List[int] = None,
        max_itemIds = 0,
        skip_history_update = False,
        detection_stages = DETECTION_STAGES
        ) -> List[int]:

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
        log(f"processing data source: {data_source_name}")
        dg = data_getter.get_data_getter(data_source)


        itemIds = dg.get_itemIds(item_names=item_names, 
                                 host_names=host_names, 
                                 group_names=group_names,
                                 itemIds=itemIds,
                                 max_itemIds=max_itemIds)
        
        d = Detector(data_source_name, data_source, itemIds)
        if not skip_history_update:
            d.update_history_stats(endep)
        
        anomaly_itemIds = []
        if STAGE_DETECT1 in detection_stages:
            log(f"running detect1 for {data_source_name}")
            anomaly_itemIds = d.detect1()
            if len(anomaly_itemIds) == 0:
                log(f"no anomalies detected for {data_source_name}")
                continue

        if STAGE_DETECT2 in detection_stages or STAGE_DETECT3 in detection_stages:
            if len(anomaly_itemIds) == 0:
                anomaly_itemIds = itemIds
            d.update_history(endep, anomaly_itemIds)

        if STAGE_DETECT2 in detection_stages:
            log(f"running detect2 for {data_source_name}")
            anomaly_itemIds = d.detect2(anomaly_itemIds, endep)
        if STAGE_DETECT3 in detection_stages:
            log(f"running detect3 for {data_source_name}")
            anomaly_itemIds = d.detect3(anomaly_itemIds, endep)

        group_map = {}
        if len(anomaly_itemIds) > 0 and len(group_names) > 0:
            group_map = dg.get_group_map(anomaly_itemIds, group_names)

        d.update_anomalies(endep, anomaly_itemIds, group_map=group_map)

    log("completed")
    return anomaly_itemIds



def classify_charts(endep: int):
    log("starting classification")
    # classify anomaly charts
    classified_itemIds = []
    conf = config_loader.conf
    data_sources = conf['data_sources']
    for data_source_name in data_sources:
        d = Detector(data_source_name, data_sources[data_source_name])
        anom = ModelsSet(data_source_name).anomalies
        anom_itemIds = anom.get_itemids()
        if len(anom_itemIds) == 0:
            continue
        d.update_history(endep, anom_itemIds)
        classified_itemIds.extend(anom_itemIds)
    if len(classified_itemIds) > 1:
        log("classifying charts")
        clusters, _, _ = dbscan.classify_charts(conf, classified_itemIds, endep=endep)
        ModelsSet(data_source_name).anomalies.update_clusterid(clusters)
    else:
        log("no anomalies")

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
    parser.add_argument('--output', type=str, help='output file', default="")
    parser.add_argument('--init', action='store_true', help='If clear DB first')
    parser.add_argument('--skip-history-update', action='store_true', help='skip to update local history')
    parser.add_argument('--trace', action='store_true', help='trace mode')
    args = parser.parse_args()

    # suppress python warnings
    import warnings
    warnings.filterwarnings("ignore")
    config = config_loader.load_config(args.config)

    if args.init:
        init(config)

    run(config, args.end, 
        item_names=args.items, 
        host_names=args.hosts, 
        group_names=args.groups,
        itemIds=args.itemids,
        skip_history_update=args.skip_history_update,
    )
    classify_charts(args.end)