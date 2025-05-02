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
import os
import pandas as pd

from data_getter.data_getter import DataGetter
import utils.config_loader as config_loader
from models.models_set import ModelsSet

class LoganGetter(DataGetter):
    fields = ['itemid', 'clock', 'value']
    loggroups_fields = ['itemid', 'count', 'score', 'text']

    def init_data_source(self, data_source_config):
        self.data_source_name = data_source_config['name']
        self.base_url = data_source_config['base_url']
        self.groups = data_source_config['groups']
        self.minimal_group_size = data_source_config.get('minimal_group_size', 1000)
        self.data: Dict[int, pd.DataFrame] = {}
        self.loggroup_data: Dict[int, pd.DataFrame] = {}
        self.itemIds = []
        self.itemId_map = {}
        self.ms = ModelsSet(self.data_source_name)
        self.endep = self.ms.history_updates.get_endep()
        self.trends_interval = config_loader.conf['trends_interval']
        self.trends_retention = config_loader.conf['trends_retention']
        self.startep = self.endep - self.trends_interval * self.trends_retention
        
        self.hosts = {}
        for g in self.groups.values():
            for hostid, host in g.items():
                self.hosts[hostid] = host
        
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        self.data_dir = data_source_config['data_dir']
        # ensure data_dir exists
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
        
        self.itemid_hostid_map = {}
        self._load_loggroups_data()
        

    def check_conn(self) -> bool:
        try:
            response = requests.get(self.base_url, headers=self.headers)
            if response.status_code == 200:
                return True
        except Exception as e:
            print(e)
        return False
    

    def initialize(self):
        self.ms.initialize()
    

    def _map_itemIds(self, hostId: int, itemIds: List[int]) -> List[int]:
        # make itemid unique by combining hostid and itemid as a string
        itemId_map = {}
        new_itemIds = []
        for itemId in itemIds:
            new_itemId = int(f"{hostId}{itemId}")
            itemId_map[itemId] = new_itemId
            new_itemIds.append(new_itemId)

        self.itemId_map.update(itemId_map)
        return new_itemIds


    def _conv_itemIds(self, df: pd.DataFrame) -> pd.DataFrame:
        # convert itemid according to itemId_map
        df['itemid'] = df['itemid'].map(self.itemId_map).fillna(df['itemid'])

        return df
    
    def _get_loggroups_data_path(self, hostid: int) -> str:
        host_name = self.hosts[hostid]
        return f"{self.data_dir}/{host_name}_loggroups.csv"
    
    def _get_loggroups_lastdata_path(self, hostid: int) -> str:
        host_name = self.hosts[hostid]
        return f"{self.data_dir}/{host_name}_loggroups_last.csv"
    
    def _load_loggroups_data(self):
        for hostid in self.hosts:
            data_path = self._get_loggroups_data_path(hostid)
            if os.path.exists(data_path):
                df = pd.read_csv(data_path)
                df.columns = self.loggroups_fields
                self._map_itemIds(hostid, df['itemid'].tolist())
                df = self._conv_itemIds(df)
                self.loggroup_data[hostid] = df
                itemIds = df['itemid'].tolist()
                itemIds = list(set(itemIds))
                for itemId in itemIds:
                    self.itemid_hostid_map[itemId] = hostid
                


    def _import_host_data(self, hostid: int):
        host_name = self.hosts[hostid]
        # get loggroups data
        url = self.base_url + '/' + host_name + '/logGroups.csv'
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            df = pd.read_csv(url)
            df.columns = self.loggroups_fields
            # save to file
            df.to_csv(self._get_loggroups_data_path(hostid), index=False)
        else:
            df = pd.DataFrame(columns=self.loggroups_fields)
        # filter by minimal_group_size
        if len(df) > 0:
            df = df[df['count'] >= self.minimal_group_size]

        url = self.base_url + '/' + host_name + '/logGroups_last.csv'
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            df_last = pd.read_csv(url)
            df_last.columns = self.loggroups_fields
            # save to file
            df_last.to_csv(self._get_loggroups_lastdata_path(hostid), index=False)
        

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

        itemIds = df['itemid'].tolist()
        itemIds = list(set(itemIds))
        itemIds = self._map_itemIds(hostid, itemIds)
        df = self._conv_itemIds(df)

        self.ms.history.upsert(df['itemid'].tolist(), df['clock'].tolist(), df['value'].tolist())
        self.endep = max(self.endep, df['clock'].max())
        self.startep = self.endep - self.trends_interval * self.trends_retention
        self.ms.history.remove_old_data(self.startep)
        

    def import_data(self):
        self.data = {}
        for hostid in self.hosts:
            self._import_host_data(hostid)
        self.ms.history_updates.upsert_updates(self.startep, self.endep)

    

    def get_itemIds(self, item_names: List[str] = [],
                    host_names: List[str] = [], 
                    itemIds: List[int] = [],
                    group_names: List[str] = [],
                    max_itemIds=0) -> List[int]:
        if len(self.loggroup_data) == 0:
            self.import_data()

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
                data = self.loggroup_data[hostid]
                itemIds2.extend(data['itemid'].tolist())

        itemIds2 = list(set(itemIds2))

        if len(itemIds) > 0:
            # filter itemIds2 by itemIds
            itemIds2 = [x for x in itemIds2 if x in itemIds]

        itemIds = itemIds2

        if max_itemIds > 0:
            itemIds = itemIds[:max_itemIds]

        return itemIds


    def get_history_data(self, startep: int, endep: int, itemIds: List[int] = []) -> pd.DataFrame:
        if endep > self.endep:
            self.import_data()
        return self.ms.history.get_data(itemIds, startep, endep)
    

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
    

    def get_items_details(self, itemIds: List[int] = []) -> pd.DataFrame:
        if len(self.loggroup_data) == 0:
            self.import_data()
        #loggroups_fields = ['itemid', 'count', 'score', 'text']
        #final df           ['group_name', 'hostid', 'host_name', 'itemid', 'item_name']
        data = pd.DataFrame(columns=['group_name', 'hostid', 'host_name', 'itemid', 'item_name'])
        for group_name, group in self.groups.items():
            for hostid, host_name in group.items():
                loggrp = self.loggroup_data[hostid]
                loggrp['group_name'] = group_name
                loggrp['hostid'] = hostid
                loggrp['host_name'] = host_name
                if len(itemIds) > 0:
                    loggrp = loggrp[loggrp['itemid'].isin(itemIds)]
                loggrp = loggrp[['group_name', 'hostid', 'host_name', 'itemid', 'text']]
                loggrp.columns = ['group_name', 'hostid', 'host_name', 'itemid', 'item_name']
                data = pd.concat([data, loggrp])
        return data
    

    def get_item_detail(self, itemId: int) -> Dict:
        data ={}
        hostid = self.itemid_hostid_map[itemId]
        loggroups_data_path = self._get_loggroups_data_path(hostid)
        if os.path.exists(loggroups_data_path):
            df = pd.read_csv(loggroups_data_path)
            df.columns = self.loggroups_fields
            df = self._conv_itemIds(df)
            data['loggroups'] = df[df['itemid'] == itemId].to_dict(orient='records')
        else:
            data['loggroups'] = pd.DataFrame(columns=self.loggroups_fields)

        loggroups_last_data_path = self._get_loggroups_lastdata_path(hostid)
        if os.path.exists(loggroups_last_data_path):
            df = pd.read_csv(loggroups_last_data_path)
            df.columns = ["groupId","lastUpdate","count","score","text"]
            df = self._conv_itemIds(df)
            data['loggroups_last'] = df[df['itemid'] == itemId].to_dict(orient='records')
        else:
            data['loggroups_last'] = pd.DataFrame(columns=self.loggroups_fields)

        return data
    
    def get_item_html_title(self, itemId: int) -> str:
        data = self.get_items_details([itemId]).iloc[0]
        return f"""<a href='/?page=details&itemid={itemId}' target='_self' style='font-size:12px;'>
            {itemId}<br>
            {data.host_name[:20]}<br>
            {data.item_name[:20]}<br>
        </a>"""