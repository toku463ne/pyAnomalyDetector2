import pandas as pd
from typing import List, Dict
import numpy as np
import utils.config_loader as config_loader
import data_getter
from models.models_set import ModelsSet
import utils


class Stats:
    data_type = ""

    def __init__(self, data_source_name, data_source, 
                        item_names: List[str] = None, 
                        host_names: List[str] = None, 
                        group_names: List[str] = None,
                        itemIds: List[int] = None,
                        max_itemIds=0):
        if item_names is None:
            item_names = []
        if host_names is None:
            host_names = []
        if group_names is None:
            group_names = []
        if itemIds is None:
            itemIds = []
        self.dg = data_getter.get_data_getter(data_source)
        self.itemIds = self.dg.get_itemIds(item_names=item_names, 
                                host_names=host_names, group_names=group_names, 
                                itemIds=itemIds,
                                max_itemIds=max_itemIds)
        self.ms = ModelsSet(data_source_name)
        

    def _get_data(self, startep: int, endep: int, itemIds: List[int]):
        if self.data_type == "trends":
            return self.dg.get_trends_data(startep=startep, endep=endep, itemIds=itemIds)
        elif self.data_type == "history":
            return self.dg.get_history_data(startep=startep, endep=endep, itemIds=itemIds)
        
    def _get_stats(self, itemIds: List[int]):
        if self.data_type == "trends":
            return self.ms.trends_stats.read_stats(itemIds)
        elif self.data_type == "history":
            return self.ms.history_stats.read_stats(itemIds)

    def _separate_existing_itemIds(self, itemIds: List[int]):
        if self.data_type == "trends":
            return self.ms.trends_stats.separate_existing_itemIds(itemIds)
        elif self.data_type == "history":
            return self.ms.history_stats.separate_existing_itemIds(itemIds)

    def _upsert_stats(self, stats: pd.DataFrame):
        ms = self.ms
        if self.data_type == "trends":
            for _, row in stats.iterrows():
                ms.trends_stats.upsert_stats(
                    row['itemid'], row['sum'], row['sqr_sum'], row['cnt'], 
                    row['mean'], row['std']
                )
        elif self.data_type == "history":
            for _, row in stats.iterrows():
                ms.history_stats.upsert_stats(
                    row['itemid'], row['sum'], row['sqr_sum'], row['cnt'], 
                    row['mean'], row['std']
                )


    def _update_stats_batch(self, itemIds: List[int], 
                                startep: int, diff_startep: int, endep: int, oldstartep: int):
        if diff_startep == 0:
            raise ValueError("diff_startep must be given")
        data = self._get_data(startep=diff_startep, endep=endep, itemIds=itemIds)
        # calculate sum, sqr_sum, count
        new_stats = data.groupby('itemid').agg(
            sum=('value', 'sum'),
            sqr_sum=('value', utils.square_sum),
            cnt=('value', 'count'),
        ).reset_index()

        if len(new_stats) == 0:
            return
        
        
        # get stats from trends_stats
        stats = self._get_stats(itemIds)
        
        if len(stats) > 0:
            # merge new stats to stats
            stats = pd.merge(stats, new_stats, on='itemid', how='inner', suffixes=('', '_new'))

            # add new stats to stats
            if len(new_stats) > 0:
                stats['sum'] = stats['sum'] + stats['sum_new']
                stats['sqr_sum'] = stats['sqr_sum'] + stats['sqr_sum_new']
                stats['cnt'] = stats['cnt'] + stats['cnt_new']

            stats = stats[['itemid', 'sum', 'sqr_sum', 'cnt']]
        else:
            stats = new_stats

        # fillna
        stats = stats.fillna(0)


        # get old data
        if oldstartep > 0 and startep != diff_startep:
            old_data = self._get_data(itemIds=itemIds, startep=oldstartep, endep=startep)
            if len(old_data) > 0:

                old_stats = old_data.groupby('itemid').agg(
                    sum=('value', 'sum'),
                    sqr_sum=('value', utils.square_sum),
                    cnt=('value', 'count')
                ).reset_index()

                # subtract old stats from stats
                if len(old_stats) > 0:
                    stats = pd.merge(stats, old_stats, on='itemid', how='outer', suffixes=('', '_old'))
                    stats = stats.fillna(0)
                    stats['sum'] = stats['sum'] - stats['sum_old']
                    stats['sqr_sum'] = stats['sqr_sum'] - stats['sqr_sum_old']
                    stats['cnt'] = stats['cnt'] - stats['cnt_old']
                    stats = stats[['itemid', 'sum', 'sqr_sum', 'cnt']]

        
        # calculate mean and std
        stats = stats.fillna(0)
        stats = stats[stats['cnt'] > 0]
        stats['mean'] = stats['sum'] / stats['cnt']
        # Adjust standard deviation calculation to match pandas' std() with Bessel's correction
        stats['std'] = np.sqrt((stats['sqr_sum'] - (np.square(stats['sum']) / stats['cnt'])) / (stats['cnt'] - 1))
        stats['std'] = stats['std'].replace([np.inf, -np.inf], np.nan).fillna(0)
        # Replace inf values in the entire DataFrame with 0
        stats.replace([np.inf, -np.inf], 0, inplace=True)


        # fillna
        stats = stats.fillna(0)

        
        # upsert stats
        self._upsert_stats(stats)


    def update_stats(self, startep: int, diff_startep: int, endep: int, oldstartep: int):
        batch_size = config_loader.conf["batch_size"]
        itemIds = self.itemIds

        existing, nonexisting = self._separate_existing_itemIds(itemIds)
        
        # import diff for existing itemIds
        for i in range(0, len(existing), batch_size):
            batch_itemIds = existing[i:i+batch_size]
            self._update_stats_batch(batch_itemIds, startep, diff_startep, endep, oldstartep)

        # import full for non existing itemIds
        for i in range(0, len(nonexisting), batch_size):
            batch_itemIds = nonexisting[i:i+batch_size]
            self._update_stats_batch(batch_itemIds, startep, startep, endep, oldstartep)
