from typing import List, Dict
import time
import logging

import utils.config_loader as config_loader
from data_processing.trends_stats import TrendsStats
from models.models_set import ModelsSet
from models.trends_updates import TrendsUpdatesModel
from models.history_updates import HistoryUpdatesModel

def log(msg, level=logging.INFO):
    msg = f"[trends_stats.py] {msg}"
    logging.log(level, msg)

def update_stats(conf: Dict, 
                endep: int, diff_startep: int =0, 
                item_names: List[str] = None, 
                host_names: List[str] = None, 
                group_names: List[str] = None,
                itemIds: List[int] = None,
                initialize: bool = False, max_itemIds = 0):
    data_sources = conf['data_sources']
    log(f"starting")

    if endep == 0:
        endep = int(time.time())
    
    # don't include the very last epoch
    endep -= 1

    # update stats
    for data_source_name in data_sources:
        data_source = data_sources[data_source_name]
        data_source["name"] = data_source_name
        log(f"processing data source: {data_source_name}")
        oldstartep: int = 0
        startep: int = 0
        diff_startep: int = 0
        ms = ModelsSet(data_source_name)
        ts = TrendsStats(data_source_name,
                         data_source=data_source, 
                         item_names=item_names, 
                         host_names=host_names, 
                         group_names=group_names, 
                         itemIds=itemIds, 
                         max_itemIds=max_itemIds)

        if initialize:
            ms.trends_updates.truncate()
            ms.trends_stats.truncate()
    
        if diff_startep == 0:
            diff_startep = ms.trends_updates.get_endep()

        oldstartep = ms.trends_updates.get_startep()

        # get old epoch from trends_interval and trends_retention
        trends_interval = conf['trends_interval']
        trends_retention = conf['trends_retention']
        startep = endep - trends_interval * trends_retention
        if diff_startep == 0:
            diff_startep = startep
        log(f"ts.update_stats({startep}, {diff_startep}, {endep}, {oldstartep})")
        ts.update_stats(startep, diff_startep, endep, oldstartep)

        ms.trends_updates.upsert_updates(startep, endep)

    log("done")


if __name__ == "__main__":
    # read arguments
    import argparse
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-c', '--config', type=str, help='config yaml file')
    parser.add_argument('--init', action='store_true', help='If clear DB first')
    args = parser.parse_args()

    # suppress python warnings
    import warnings
    warnings.filterwarnings("ignore")
    config = config_loader.load_config(args.config)
    
    update_stats(config, 0, initialize=args.init)
