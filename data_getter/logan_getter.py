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
    history_fields = ['itemid', 'clock', 'value']
    loggroups_fields = ['itemid', 'count', 'score', 'text']
    last_loggroups_fields = ["itemid","lastUpdate","count","score","text"]

    def init_data_source(self, data_source_config):
        self.data_source_name = data_source_config['name']
        self.base_url = data_source_config['base_url']
        self.groups = data_source_config['groups']
        self.minimal_group_size = data_source_config.get('minimal_group_size', 1000)
        self.data: Dict[int, pd.DataFrame] = {}
        self.item_details: pd.DataFrame = pd.DataFrame(columns=['group_name', 'hostid', 'host_name', 'itemid', 'item_name'])
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
      
   

    def _get_data_by_http(self, hostid: int, file_name: str, 
                          columns: List[str],
                          write_to_csv = False) -> pd.DataFrame:
        url = f"{self.base_url}/{self.hosts[hostid]}/{file_name}"
        csv_path = f"{self.data_dir}/{self.hosts[hostid]}_{file_name}"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            df = pd.read_csv(url)
            df.columns = columns
            # save to file
            if write_to_csv:
                df.to_csv(csv_path, index=False)
            return df
        else:
            return pd.DataFrame(columns=columns)
    
    
    
    def _load_loggroups_data(self):
        for hostid in self.hosts:
            data_path = f"{self.data_dir}/{self.hosts[hostid]}_loggroups.csv"
            if os.path.exists(data_path):
                df = pd.read_csv(data_path)
                df.columns = self.loggroups_fields
                self._map_itemIds(hostid, df['itemid'].tolist())
                df = self._conv_itemIds(df)
                itemIds = df['itemid'].tolist()
                itemIds = list(set(itemIds))
                # import to item details
                group_names = self.get_groups_by_hostid(hostid)
                if len(group_names) > 0:
                    for group_name in group_names:
                        self._load_item_details(df, group_name, hostid)
        

    def _load_item_details(self, lgdf: pd.DataFrame, group_name: str, hostid: int):
        # lgdf.columns = ['itemid', 'count', 'score', 'text']
        # details fields = ['group_name', 'hostid', 'host_name', 'itemid', 'item_name']
        detaildf = lgdf[['itemid', 'text']].copy()
        detaildf['group_name'] = group_name
        detaildf['hostid'] = hostid
        detaildf['host_name'] = self.hosts[hostid]
        detaildf.columns = ['itemid', 'item_name', 'group_name', 'hostid', 'host_name']
        detaildf = detaildf[['group_name', 'hostid', 'host_name', 'itemid', 'item_name']]
        detaildf['item_count'] = lgdf['count']
        self.item_details = pd.concat([self.item_details, detaildf], ignore_index=True)

        # load itemid_hostid_map
        self.itemid_hostid_map.update({row['itemid']: hostid for _, row in detaildf.iterrows()})

    def get_groups_by_hostid(self, hostid: int) -> List[str]:
        # get groups by hostid
        groups = []
        for group_name, group in self.groups.items():
            if hostid in group:
                groups.append(group_name)
        return groups

    def _import_host_data(self, hostid: int):
        lgdf = self._get_data_by_http(hostid, 'logGroups.csv', self.loggroups_fields, True)

        # filter by minimal_group_size
        if len(lgdf) > 0:
            lgdf = lgdf[lgdf['count'] >= self.minimal_group_size]

        if len(lgdf) == 0:
            return

        # get itemids
        itemIds = lgdf['itemid'].tolist()
        itemIds = list(set(itemIds))

        # map to unique itemids
        itemIds = self._map_itemIds(hostid, itemIds)
        self.itemIds.extend(itemIds)

        # convert itemids to unique itemids
        lgdf = self._conv_itemIds(lgdf)

        # import to item details
        group_names = self.get_groups_by_hostid(hostid)
        if len(group_names) > 0:
            for group_name in group_names:
                self._load_item_details(lgdf, group_name, hostid)

        self._get_data_by_http(hostid, 'logGroups_last.csv', self.last_loggroups_fields, True)

        
        # get metrics data
        df = self._get_data_by_http(hostid, 'history.csv', self.history_fields, False)
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
        if len(self.item_details) == 0:
            self.import_data()

        itemIds = itemIds if len(itemIds) > 0 else self.itemIds
        # filter by host_names
        host_names = host_names if len(host_names) > 0 else self.hosts.values()
        # filter host_names by group_names
        group_names = group_names if len(group_names) > 0 else self.groups.keys()

        itemIds2 = []
        if len(item_names) > 0:
            # filter item_details by item_names
            item_details = self.item_details[self.item_details['item_name'].isin(item_names)]
            itemIds2 = item_details['itemid'].tolist()
        else:
            itemIds2 = self.item_details['itemid'].tolist()
        
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
        if len(self.item_details) == 0:
            self.import_data()
            
        if len(itemIds) == 0:
            return self.item_details
        else:
            return self.item_details[self.item_details['itemid'].isin(itemIds)]

    

    def get_item_detail(self, itemId: int) -> Dict:
        if len(self.item_details) == 0:
            self.import_data()
        data ={}
        hostid = self.itemid_hostid_map[itemId]
        file_name = "logGroups.csv"
        loggroups_data_path = f"{self.data_dir}/{self.hosts[hostid]}_{file_name}"
        if os.path.exists(loggroups_data_path):
            df = pd.read_csv(loggroups_data_path)
            df.columns = self.loggroups_fields
            df = self._conv_itemIds(df)
            data = df[df['itemid'] == itemId].to_dict(orient='records')[0]
        

        file_name = "logGroups_last.csv"
        loggroups_last_data_path = f"{self.data_dir}/{self.hosts[hostid]}_{file_name}"
        if os.path.exists(loggroups_last_data_path):
            df = pd.read_csv(loggroups_last_data_path)
            df.columns = self.last_loggroups_fields
            df = self._conv_itemIds(df)
            last_data = df[df['itemid'] == itemId].to_dict(orient='records')[0]
        
        data["last_text"] = last_data["text"]
        data["host_name"] = self.hosts[hostid]

        return data
    
    def get_item_html_title(self, itemId: int, chart_type="") -> str:
        data = self.get_items_details([itemId])
        print({'itemId': itemId, 'data': data})
        data = data.iloc[0]
        href = f"/?page=details&itemid={itemId}"
        if chart_type == "topitems":
            href += f"&chart_type={chart_type}"

        return f"""<a href='{href}' style='font-size:12px; target="_blank"'>
            {itemId}<br>
            {data.host_name[:20]}<br>
            {data.item_name[:20]}<br>
        </a>"""