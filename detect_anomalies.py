from typing import Dict, List, Tuple
import logging
import time


import utils.config_loader as config_loader
from models.models_set import ModelsSet
import data_processing.detector as detector
from data_processing.history_stats import HistoryStats
import data_getter
import utils.normalizer as normalizer
from data_processing.detector import Detector

STAGE_DETECT1 = 1
STAGE_DETECT2 = 2
STAGE_DETECT3 = 3
DETECTION_STAGES = [
    STAGE_DETECT1,
    STAGE_DETECT2,
    STAGE_DETECT3,
]

def log(msg, level=logging.INFO):
    msg = f"[detector.py] {msg}"
    logging.log(level, msg)

def run(conf: Dict, endep: int = 0, 
        item_names: List[str] = None, 
        host_names: List[str] = None, 
        group_names: List[str] = None,
        itemIds: List[int] = None,
        max_itemIds = 0,
        initialize = False,
        skip_history_update = False,
        detection_stages = DETECTION_STAGES
        ) -> List[int]:

    
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
        log(f"processing data source: {data_source_name}")
        dg = data_getter.get_data_getter(data_source)


        itemIds = dg.get_itemIds(item_names=item_names, 
                                 host_names=host_names, 
                                 group_names=group_names,
                                 itemIds=itemIds,
                                 max_itemIds=max_itemIds)
        
        d = Detector(data_source_name, data_source, itemIds)
        if not skip_history_update:
            d.update_history_stats(endep, initialize=initialize)
        
        anomaly_itemIds = []
        if STAGE_DETECT1 in detection_stages:
            log(f"running detect1 for {data_source_name}")
            anomaly_itemIds = d.detect1()

        d.insert_anomalies(anomaly_itemIds, created=endep)


    return anomaly_itemIds

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

    run(config, args.end, 
        item_names=args.items, 
        host_names=args.hosts, 
        group_names=args.groups,
        itemIds=args.itemids,
        initialize=args.init,
        skip_history_update=args.skip_history_update,
    )