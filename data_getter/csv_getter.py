"""
class to get data from CSV files
"""
import os, json, csv, gzip

from data_getter.data_getter import DataGetter
from typing import Dict, List
import pandas as pd # type: ignore

class CsvGetter(DataGetter):
    fields = ['itemid', 'clock', 'value']
    fields_full = ['itemid', 'clock', 'value_min', 'value_avg', 'value_max']
    trends_filename = 'trends.csv.gz'
    history_filename = 'history.csv.gz'
    items_filename = 'items.csv.gz' # group_name, hostid, itemid
    
    def init_data_source(self, data_source_config):
        self.data_dir = data_source_config['data_dir']
        

    def check_conn(self) -> bool:
        # check if the data_dir exists
        return os.path.exists(self.data_dir)
    
    def get_history_data(self, startep: int, endep: int, itemIds: List[int] = []) -> pd.DataFrame:
        df = pd.read_csv(os.path.join(self.data_dir, self.history_filename), header=0)
        if len(df) == 0:
            return pd.DataFrame(columns=self.fields)
        df.columns = self.fields

        # remove rows with str values
        df = df[df['clock'] != 'clock']

        # convert itemid to int
        df['itemid'] = df['itemid'].astype(int)
        # convert clock to int
        df['clock'] = df['clock'].astype(int)
        # convert value to float
        df['value'] = df['value'].astype(float)

        # filter by time
        # Ensure 'clock' column is numeric and drop invalid rows
        df['clock'] = pd.to_numeric(df['clock'], errors='coerce')
        df = df.dropna(subset=['clock'])
        df['clock'] = df['clock'].astype(int)

        # Filter by time
        # Ensure 'clock' column is numeric and drop invalid rows
        df['clock'] = pd.to_numeric(df['clock'], errors='coerce')
        df = df.dropna(subset=['clock'])
        df['clock'] = df['clock'].astype(int)

        # Filter by time
        # Ensure 'clock' column is numeric and drop invalid rows
        df['clock'] = pd.to_numeric(df['clock'], errors='coerce')
        df = df.dropna(subset=['clock'])
        df['clock'] = df['clock'].astype(int)

        # Filter by time
        df = df[(df['clock'] >= startep)]
        df = df[(df['clock'] <= endep)]

        # filter by itemIds
        if len(itemIds) > 0:
            df = df[df['itemid'].isin(itemIds)]

        # sort by itemid, clock
        df = df.sort_values(['itemid', 'clock'])
        return df
    
    def get_trends_data(self, startep: int, endep: int, itemIds: List[int] = []) -> pd.DataFrame:
        df = self.get_trends_full_data(startep, endep, itemIds)
        # convert value_avg to value
        df['value'] = df['value_avg']
        # sort by itemid, clock
        df = df.sort_values(['itemid', 'clock'])
        return df[self.fields]
    
    
    def get_trends_full_data(self, startep: int, endep: int, itemIds: List[int] = []) -> pd.DataFrame:
        df = pd.read_csv(os.path.join(self.data_dir, self.trends_filename))
        if len(df) == 0:
            return pd.DataFrame(columns=self.fields_full)
        df.columns = self.fields_full
        # remove rows including string 'clock' at clock
        df = df[df['clock'] != 'clock']
        df.fillna(0, inplace=True)
        # convert itemid to int
        df['itemid'] = df['itemid'].astype(int)
        # convert clock to int
        df['clock'] = df['clock'].astype(int)
        # convert value_avg to float
        df['value_avg'] = df['value_avg'].astype(float)
        # convert value_min to float
        df['value_min'] = df['value_min'].astype(float)
        # convert value_max to float
        df['value_max'] = df['value_max'].astype(float)

        df['clock'] = pd.to_numeric(df['clock'], errors='coerce')
        df = df.dropna(subset=['clock'])
        df['clock'] = df['clock'].astype(int)
        
        # filter by time
        df = df[(df['clock'] >= startep) & (df['clock'] <= endep)]

        # filter by itemIds
        if len(itemIds) > 0:
            df = df[df['itemid'].isin(itemIds)]

        # sort by itemid, clock
        df = df.sort_values(['itemid', 'clock'])
        return df
    
    def get_itemIds(self, item_names: List[str] = [], 
                    host_names: List[str] = [], 
                    group_names: List[str] = [],
                    max_itemIds = 0,
                    itemIds: List[int] = []) -> List[int]:
        df = pd.read_csv(os.path.join(self.data_dir, self.history_filename))
        results = df['itemid'].unique().tolist()     

        # filter by itemIds
        if len(itemIds) > 0 and len(results) > 0:
            results = [itemid for itemid in results if itemid in itemIds]
        
        if max_itemIds > 0:
            results = results[:max_itemIds]
        return results


    def classify_by_groups(self, itemIds: List[int], group_names: List[str]) -> Dict[str, List[int]]:
        items = {}
        with gzip.open(os.path.join(self.data_dir, self.items_filename), 'rt') as f:
            csvreader = csv.DictReader(f)
            for row in csvreader:
                if int(row['itemid']) not in itemIds:
                    continue
                items[row['itemid']] = {
                    'group_name': row['group_name'],
                    'hostid': row['hostid']
                }

        groups = {}
        for group_name in group_names:
            groups[group_name] = [int(itemid) for itemid, item in items.items() if item['group_name'] == group_name]

        return groups
    
    
    def get_items_details(self, itemIds: List[int]) -> pd.DataFrame:
        # open items.csv.gz into dataframe
        df = pd.read_csv(os.path.join(self.data_dir, self.items_filename), compression='gzip')
        df.columns = ['group_name', 'hostid', 'host_name', 'itemid', 'item_name']
        # filter by itemIds
        if len(itemIds) > 0:
            df = df[df['itemid'].isin(itemIds)]

        return df
    

    def get_item_host_dict(self, itemIds: List[int]=[]) -> Dict[int, int]:
        items = {}
        # open items.csv.gz
        with gzip.open(os.path.join(self.data_dir, self.items_filename), 'rt') as f:
            csvreader = csv.DictReader(f)
            for row in csvreader:
                itemId = int(row['itemid'])
                if itemId not in itemIds:
                    continue
                items[itemId] = int(row['hostid'])
        return items
    

    def get_group_map(self, itemIds: List[int], group_names: List[str]) -> Dict[int, str]:
        if len(itemIds) == 0:
            return {}
        
        if len(group_names) == 0:
            return {}

        df = pd.read_csv(os.path.join(self.data_dir, self.items_filename), compression='gzip')
        #group_name,hostid,host_name,itemid,item_name
        df.columns = ['group_name', 'hostid', 'host_name', 'itemid', 'item_name']

        group_map = {}
        for row in df.itertuples():
            itemId = int(row.itemid)
            if itemId not in itemIds:
                continue
            group_name = row.group_name
            if group_name not in group_names:
                continue
            group_map[itemId] = group_name

        return group_map


    
    # funtion to classify items by host groups
    def classify_by_groups(self, itemIds: List[int], group_names: List[str]) -> dict:
        items = {}
        with gzip.open(os.path.join(self.data_dir, self.items_filename), 'rt') as f:
            csvreader = csv.DictReader(f)
            for row in csvreader:
                if int(row['itemid']) not in itemIds:
                    continue
                items[row['itemid']] = {
                    'group_name': row['group_name'],
                    'hostid': row['hostid']
                }

        groups = {}
        if len(group_names) == 0:
            groups['all'] = [int(itemid) for itemid, item in items.items()]
        for group_name in group_names:
            groups[group_name] = [int(itemid) for itemid, item in items.items() if item['group_name'][:len(group_name)] == group_name]

        return groups
        