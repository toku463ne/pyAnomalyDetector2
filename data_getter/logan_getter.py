"""
class to get metrics from URL
Expected structure in the URL:
    base_url/
    |- host1/history.csv
    |- host2/history.csv
    |- host3/history.csv
Define host_names and group_names structure in the config file (yaml)
example:
    groups:
        group1:
            1: host1
            2: host2
        group2:
            3: host3
"""
from typing import Dict, List
import requests
import json
import pandas as pd

from data_getter.data_getter import DataGetter
import utils.config_loader as config_loader

class LoganGetter(DataGetter):
    fields = ['itemid', 'clock', 'value']
    loggroups_fields = ['itemid', 'count', 'score', 'text']

    def init_data_source(self, data_source_config):
        config = config_loader.conf
        self.base_url = data_source_config['base_url']
        self.groups = data_source_config['groups']
        self.minimal_group_size = data_source_config.get('minimal_group_size', 1000)
        self.data: Dict[str, pd.DataFrame] = {}
        self.loggroup_data: Dict[str, pd.DataFrame] = {}
        self.itemIds = []
        self.itemId_map = {}
        
        self.hosts = {}
        for g in self.groups.values():
            for hostid, host in g.items():
                self.hosts[hostid] = host
        
        self.data_loaded = False
        self.trends_interval = config['trends_interval']
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }


    def check_conn(self) -> bool:
        try:
            response = requests.get(self.base_url, headers=self.headers)
            if response.status_code == 200:
                return True
        except Exception as e:
            print(e)
        return False
    

    def _map_itemIds(self, hostId: int, itemIds: List[int]) -> List[str]:
        # make itemid unique by combining hostid and itemid as a string
        itemId_map = {}
        new_itemIds = []
        for itemId in itemIds:
            new_itemId = f"{hostId}{itemId}"
            itemId_map[itemId] = new_itemId
            new_itemIds.append(new_itemId)

        self.itemId_map.update(itemId_map)
        return new_itemIds


    def _conv_itemIds(self, df: pd.DataFrame) -> pd.DataFrame:
        # convert itemid according to itemId_map
        df['itemid'] = df['itemid'].map(self.itemId_map).fillna(df['itemid']).astype(str)

        return df


    def _load_host_data(self, hostid: str):
        host_name = self.hosts[hostid]
        # get loggroups data
        url = self.base_url + '/' + host_name + '/logGroups.csv'
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            df = pd.read_csv(url)
            df.columns = self.loggroups_fields
        else:
            df = pd.DataFrame(columns=self.loggroups_fields)
        # filter by minimal_group_size
        if len(df) > 0:
            df = df[df['count'] >= self.minimal_group_size]

        # get itemids
        itemIds = df['itemid'].tolist()
        itemIds = list(set(itemIds))
        itemIds = self._map_itemIds(hostid, itemIds)

        self.itemIds.extend(itemIds)

        df = self._conv_itemIds(df)
        self.loggroup_data[hostid] = df

        
        # get metrics data
        url = self.base_url + '/' + host_name + '/history.csv'
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            df = pd.read_csv(url)
            df.columns = self.fields
        else:
            df = pd.DataFrame(columns=self.fields)

        df = self._conv_itemIds(df)

        # filter by itemIds
        if len(df) > 0:
            df = df[df['itemid'].astype(str).isin([str(i) for i in itemIds])]

        self.data[hostid] = df


    def _load_data(self, force: bool = False):
        if not force and self.data_loaded:
            return

        self.data = {}
        for hostid in self.hosts:
            self._load_host_data(hostid)
        self.data_loaded = True

    def get_itemIds(self, item_names: List[str] = [],
                    host_names: List[str] = [], 
                    itemIds: List[int] = [],
                    group_names: List[str] = [],
                    max_itemIds=0) -> List[int]:
        if len(self.data) == 0:
            self._load_data()
        itemIds = itemIds if len(itemIds) > 0 else self.itemIds
        # filter by host_names
        host_names = host_names if len(host_names) > 0 else self.hosts.values()
        # filter host_names by group_names
        group_names = group_names if len(group_names) > 0 else self.groups.keys()

        itemIds2 = []
        if len(item_names) > 0:
            # filter loggroups_data['text'] by item_names regex
            for hostid in self.hosts:
                data = self.loggroup_data[hostid]
                itemIds2.extend(data[data['text'].str.contains('|'.join(item_names))]['itemid'].tolist())
        else:
            for hostid in self.hosts:
                data = self.data[hostid]
                itemIds2.extend(data['itemid'].tolist())

        itemIds2 = list(set(itemIds2))

        if len(itemIds) > 0:
            # filter itemIds2 by itemIds
            itemIds2 = [x for x in itemIds2 if x in itemIds]

        itemIds = itemIds2

        if max_itemIds > 0:
            itemIds = itemIds[:max_itemIds]

        return itemIds
    
    def get_history_data(self, startep, endep, itemIds = []):
        if len(self.data) == 0:
            self._load_data()
        data = pd.DataFrame(columns=self.fields)
        for hostid in self.hosts:
            data = pd.concat([data, self.data[hostid]])
        # filter by itemIds
        if len(itemIds) > 0:
            data = data[data['itemid'].astype(str).isin([str(i) for i in itemIds])]
        # filter by time
        data = data[(data['clock'] >= startep) & (data['clock'] <= endep)]
        return data
    
    def get_trends_data(self, startep, endep, itemIds = []):
        data = self.get_history_data(startep, endep, itemIds)
        # sum values by trends_interval
        data['clock'] -= data['clock'] % self.trends_interval
        data = data.groupby(['itemid', 'clock']).agg(value=('value', 'mean'), count=('value', 'count')).reset_index()
        data.columns = ['itemid', 'clock', 'value', 'count']
        return data
    
    def get_trends_full_data(self, startep, endep, itemIds = []):
        data = self.get_history_data(startep, endep, itemIds)
        # sum values by trends_interval, use the first clock
        data['clock'] -= data['clock'] % self.trends_interval
        data = data.groupby(['itemid', 'clock']).agg({'value': ['min', 'mean', 'max']}).reset_index()
        return data
    
    def get_items_details(self, itemIds) -> pd.DataFrame:
        #loggroups_fields = ['itemid', 'count', 'score', 'text']
        #final df           ['group_name', 'hostid', 'host_name', 'itemid', 'item_name']
        data = pd.DataFrame(columns=['group_name', 'hostid', 'host_name', 'itemid', 'item_name'])
        for group_name, group in self.groups.items():
            for hostid, host_name in group.items():
                loggrp = self.loggroup_data[hostid]
                loggrp['group_name'] = group_name
                loggrp['hostid'] = hostid
                loggrp['host_name'] = host_name
                loggrp = loggrp[loggrp['itemid'].isin(itemIds)]
                loggrp = loggrp[['group_name', 'hostid', 'host_name', 'itemid', 'text']]
                loggrp.columns = ['group_name', 'hostid', 'host_name', 'itemid', 'item_name']
                data = pd.concat([data, loggrp])
        return data