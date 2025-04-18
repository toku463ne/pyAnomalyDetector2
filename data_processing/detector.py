import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
import logging
import time

import utils
import utils.config_loader as config_loader
import data_getter
from models.models_set import ModelsSet
from data_processing.history_stats import HistoryStats



def log(msg, level=logging.INFO):
    msg = f"[data_processing/detector.py] {msg}"
    logging.log(level, msg)


class Detector:
    def __init__(self, data_source_name, data_source: Dict, 
                 itemIds: List[int] = [], 
                 max_itemIds: int = 0,
                skip_history_update: bool = False,
                 ):
        config = config_loader.conf
        self.skip_history_update = skip_history_update
        self.batch_size = config['batch_size']
        self.detect1_lambda_threshold = config['detect1_lambda_threshold']
        self.trends_min_count = config['trends_min_count']
        self.ignore_diff_rate = config['ignore_diff_rate']
        self.history_interval = config["history_interval"]
        self.trends_interval = config["trends_interval"]
        self.history_retention = config["history_retention"]
        self.history_recent_retention = config["history_recent_retention"]
        self.trends_retention = config["trends_retention"]
        
        self.data_source = data_source
        self.data_source_name = data_source_name
        self.item_conds = data_source.get("item_conds", {})
        self.item_diff_conds = data_source.get("item_diff_conds", {})
        
        self.dg = data_getter.get_data_getter(data_source)
        self.ms = ModelsSet(data_source_name)
        self.max_itemIds = max_itemIds
        
        if len(itemIds) == 0:
            itemIds = self.dg.get_itemIds()
        if len(itemIds) == 0:
            raise ValueError("No itemIds found in data source")
        self.itemIds = itemIds



    def update_history_stats(self, endep: int = 0, initialize: bool = False,) -> None:
        ms = self.ms
        itemIds = self.itemIds
        history_interval = self.history_interval
        history_retention = self.history_retention
        oldstartep = ms.history_updates.get_startep()
        if endep == 0:
            endep = self.endep
        # get start time
        startep = endep - history_retention * history_interval
        
        if initialize:
            ms.history.truncate()
            ms.history_stats.truncate()
            ms.history_updates.truncate()
            ms.anomalies.truncate()
        

        # only itemIds existing in trends_stats table
        itemIds, _ = ms.trends_stats.separate_existing_itemIds(itemIds)
        if len(itemIds) == 0:
            log("No itemIds found in trends_stats table")
            return
        
        diff_startep = 0
        oldendep = ms.history_updates.get_endep()
        if oldendep > 0:
            if startep > oldendep + history_interval*2:
                ms.history_updates.truncate()
                ms.history.truncate()
                ms.history_stats.truncate()
                diff_startep = startep
            else:
                diff_startep = oldendep + 1
        else:
            diff_startep = startep

        hs = HistoryStats(
                            data_source_name=self.data_source_name,
                            data_source=self.data_source, 
                            itemIds=itemIds, 
                            max_itemIds=self.max_itemIds)
        if self.skip_history_update == False:
            #ms.history.truncate()
            log(f"hs.update_stats({startep}, {diff_startep}, {endep}, {oldstartep})")
            hs.update_stats(startep, diff_startep, endep, oldstartep)

        # update history_updates table
        ms.history_updates.upsert_updates(startep, endep)


        
    def detect1(self) -> List[int]:
        batch_size = self.batch_size
        ms = self.ms
        itemIds = self.itemIds
        
        log(f"detector.detect1: itemIds: {len(itemIds)}")

        anomaly_itemIds = []
        for i in range(0, len(itemIds), batch_size):
            batch_itemIds = itemIds[i:i + batch_size]
            t_stats = ms.trends_stats.read_stats(batch_itemIds)[['itemid', 'mean', 'std', 'cnt']]
            batch_anomaly_itemIds = self.detect1_batch(batch_itemIds, t_stats)
            if len(batch_anomaly_itemIds) > 0:
                anomaly_itemIds += batch_anomaly_itemIds

        log(f"detector.detect1: found anomalies: {len(anomaly_itemIds)}")
        return anomaly_itemIds


    def _evaluate_cond(self, value: float, cond: Dict) -> bool:
        if "condition" not in cond.keys():
            return False

        operator = cond["condition"].get("operator", "")
        threshold = cond["condition"].get("value", "")
        if operator == ">":
            return value > threshold
        elif operator == "<":
            return value < threshold
        elif operator == "=":
            return value == threshold
        elif operator == ">=":
            return value >= threshold
        elif operator == "<=":
            return value <= threshold
        return False


    def detect1_batch(self, itemIds: List[int], 
        t_stats: pd.DataFrame) -> List[int]:
        ms = self.ms
        detect1_lambda_threshold = self.detect1_lambda_threshold
        trends_min_count = self.trends_min_count
        ignore_diff_rate = self.ignore_diff_rate
        means = ms.history_stats.read_stats(itemIds)[['itemid', 'mean']]

        # get stats
        #t_stats = ms.trends_stats.read_stats(itemIds)[['itemid', 'mean', 'std', 'cnt']]
        t_stats = t_stats[t_stats['cnt'] > trends_min_count]

        # merge stats
        h_stats_df = pd.merge(means, t_stats, on='itemid', how='inner', suffixes=('_h', '_t'))
        h_stats_df = h_stats_df[h_stats_df['std']>0]

        # filter h_stats_df where mean_h > mean_t + lambda1_threshold * std_t | mean_h < mean_t - lambda1_threshold * std_t
        h_stats_df = h_stats_df[(h_stats_df['mean_h'] > h_stats_df['mean_t'] + detect1_lambda_threshold * h_stats_df['std']) | (h_stats_df['mean_h'] < h_stats_df['mean_t'] - detect1_lambda_threshold * h_stats_df['std'])]


        if len(h_stats_df) == 0:
            return []

        # ignore small diffs
        h_stats_df = h_stats_df[h_stats_df['mean_t'] > 0 & (abs(h_stats_df['mean_h'] - h_stats_df['mean_t'])/h_stats_df['mean_t'] > ignore_diff_rate)]


        if len(h_stats_df) == 0:
            return []

        # get itemIds
        itemIds = h_stats_df['itemid'].tolist()
        itemIds = list(set(itemIds))

        dg = self.dg

        # filter by defined conds
        #log(f"detector.filter_by_cond(itemIds)")
        item_conds = self.item_conds
        if len(item_conds) > 0 and len(itemIds) > 0:
            for cond in item_conds:
                #if cond['filter'] == "key_ LIKE 'vmware.vm.guest.osuptime%'":
                #    print("")
                if len(itemIds) == 0:
                    break
                itemIds2 = dg.check_itemId_cond(itemIds, cond["filter"])
                for itemId in itemIds2:    
                    value = means[means['itemid'] == itemId].iloc[0]['mean']
                    if self._evaluate_cond(value, cond) == False:
                        itemIds.remove(itemId)


        if len(itemIds) == 0:
            return []

        # filter by defined diff conds
        item_diff_conds = self.item_diff_conds
        h_stats_df['diff'] = abs(h_stats_df['mean_h'] - h_stats_df['mean_t'])

        if len(item_diff_conds) > 0 and len(itemIds) > 0:
            for cond in item_diff_conds:
                if len(itemIds) == 0:
                    break
                itemIds2 = dg.check_itemId_cond(itemIds, cond['filter'])
                for itemId in itemIds2:
                    #if cond['filter'] == "key_ LIKE 'vmware.vm.guest.osuptime%'":
                    #    print("")
                    value = h_stats_df[h_stats_df['itemid'] == itemId].iloc[0]['diff']
                    if self._evaluate_cond(value, cond) == False:
                        itemIds.remove(itemId)
        
        return itemIds
    

    def _get_df(self, itemIds: List[int], t_start: int, h_start: int, h_end: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
        dg = self.dg
        ms = self.ms
        trends_df = dg.get_trends_full_data(itemIds=itemIds, startep=t_start, endep=h_start)
        if trends_df.empty:
            return pd.DataFrame()
        history_df = ms.history.get_data(itemIds, startep=h_start, endep=h_end)
        if history_df.empty:
            return pd.DataFrame()

        return trends_df, history_df


    def _detect_diff_anomalies(self, itemIds: List[int], 
                            trends_df: pd.DataFrame, recent_stats: pd.DataFrame, 
                            lamnda_threshold: float,
                            is_up=True) -> List[int]:
        ignore_diff_rate = self.ignore_diff_rate
        if is_up:
            trends_df2 = trends_df[['itemid', 'clock', 'value_max']]
            trends_df2.columns = ['itemid', 'clock', 'value']
        else:
            trends_df2 = trends_df[['itemid', 'clock', 'value_min']]
            trends_df2.columns = ['itemid', 'clock', 'value']


        # create a dataframe with adjacent value_xx peaks from trends_df
        trends_diff = pd.DataFrame(columns=['itemid', 'clock', 'value', 'diff'], dtype=object)
        for itemId in itemIds:
            df = trends_df2[trends_df2['itemid'] == itemId]
            df = df.copy()
            df['diff'] = df['value'].diff().fillna(0)
            df = df[df['diff'] != 0]
            if trends_diff.empty:
                trends_diff = df
            elif not df.empty and len(df) > 0:
                trends_diff = pd.concat([trends_diff, df])

        # calculate trends_diff mean and std
        trends_diff_stats = trends_diff.groupby('itemid')['diff'].agg(['mean', 'std']).reset_index()
        #recent_stats = recent_diff.groupby('itemid')['diff'].agg(['min', 'max']).reset_index()
        
        # merge with hist_stats by itemid
        stats_df = pd.merge(recent_stats, trends_diff_stats, on='itemid', how='inner')
        stats_df = stats_df[stats_df['std'] > 0]

        if is_up:
            stats_df['diff'] = abs(stats_df['max'] - stats_df['mean'])
            # filter by lambda_threshold
            stats_df = stats_df[stats_df['diff'] > lamnda_threshold * stats_df['std']]

            # filter by ignore_diff_rate
            stats_df = stats_df[abs(stats_df['max'] - stats_df['mean'])/stats_df['mean'] > ignore_diff_rate]
        else:
            stats_df['diff'] = abs(stats_df['mean'] - stats_df['min'])

            # filter by lambda_threshold
            stats_df = stats_df[stats_df['diff'] > lamnda_threshold * stats_df['std']]
            # filter by ignore_diff_rate
            stats_df = stats_df[abs(stats_df['min'] - stats_df['mean'])/stats_df['mean'] > ignore_diff_rate]

        # get itemIds
        itemIds = stats_df['itemid'].tolist()

        return itemIds


    def _detect2_batch(self, history_df: pd.DataFrame, trends_df: pd.DataFrame,
            itemIds: List[int]) -> List[int]:
        
        # group by itemid and get min, max and the first value
        r_stats = history_df.groupby('itemid')['value'].agg(['min', 'max', 'first']).reset_index()
        r_stats["min_diff"] = r_stats["min"] - r_stats["first"]
        r_stats["max_diff"] = r_stats["max"] - r_stats["first"]
        r_stats = r_stats[['itemid', 'min_diff', 'max_diff']]
        r_stats.columns = ['itemid', 'min', 'max']


        itemIds_up = self._detect_diff_anomalies(itemIds, trends_df, r_stats, self.detect2_lambda_threshold, is_up=True)
        itemIds_dw = self._detect_diff_anomalies(itemIds, trends_df, r_stats, self.detect2_lambda_threshold, is_up=False)

        itemIds = itemIds_up + itemIds_dw
        itemIds = list(set(itemIds))
        return itemIds


    def detect2(self, itemIds: List[int]) -> List[int]:
        batch_size = self.batch_size
        itemIds = self.itemIds        
        log(f"detector.detect2: itemIds: {len(itemIds)}")

        anomaly_itemIds = []
        for i in range(0, len(itemIds), batch_size):
            batch_itemIds = itemIds[i:i+batch_size]
            trends_df, history_df = self._get_df(batch_itemIds, t_start=self.trends_interval, h_start=self.history_interval, h_end=self.history_retention)
            if trends_df.empty or history_df.empty:
                continue

            anomaly_itemIds.extend(self._detect2_batch(history_df, trends_df, batch_itemIds))

        log(f"detector.detect2: found anomalies: {len(anomaly_itemIds)}")
        return anomaly_itemIds
    
    
    def detect3(self, itemIds: List[int]) -> List[int]:
        batch_size = self.batch_size
        itemIds = self.itemIds        
        log(f"detector.detect3: itemIds: {len(itemIds)}")

        anomaly_itemIds = []
        for i in range(0, len(itemIds), batch_size):
            batch_itemIds = itemIds[i:i+batch_size]
            trends_df, history_df = self._get_df(batch_itemIds, t_start=self.trends_interval, h_start=self.history_interval, h_end=self.history_retention)
            if trends_df.empty or history_df.empty:
                continue

            anomaly_itemIds.extend(self._detect3_batch(trends_df, base_clocks, batch_itemIds, startep2, 
                                                lambda3_threshold, lambda4_threshold))

        log(f"detector.detect3: found anomalies: {len(anomaly_itemIds)}")
        return anomaly_itemIds




    def insert_anomalies(self, itemIds: List[int], created: int):
        dg = self.dg
        ms = self.ms
        df = dg.get_items_details(itemIds)
        # df.columns = ['group_name', 'hostid', 'host_name', 'itemid', 'item_name']

        # get trends stats
        trends_stats = ms.trends_stats.read_stats(itemIds)
        trends_stats = trends_stats[['itemid', 'mean', 'std']]
        # merge with df
        df = pd.merge(df, trends_stats, on='itemid', how='inner')
        # rename columns
        df.columns = ['group_name', 'hostid', 'host_name', 'itemid', 'item_name', 'trend_mean', 'trend_std']

        df['created'] = created
        df['clusterid'] = -1
        df['hostid'] = df['hostid'].astype(int)
        df['itemid'] = df['itemid'].astype(int)
        df['group_name'] = df['group_name'].astype(str)
        df['host_name'] = df['host_name'].astype(str)
        df['item_name'] = df['item_name'].astype(str)
        df['trend_mean'] = df['trend_mean'].astype(float)
        df['trend_std'] = df['trend_std'].astype(float)
        df['created'] = df['created'].astype(int)
        
        ms.anomalies.insert_data(df)