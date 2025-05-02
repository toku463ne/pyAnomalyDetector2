"""
Super class to get data from different sources
"""
from typing import List, Dict, Tuple
from abc import abstractmethod
import pandas as pd # type: ignore

class DataGetter:
    def __init__(self, data_source_config):
        self.data_source_config = data_source_config
        self.init_data_source(data_source_config)

    # function to initialize the data source. 
    @abstractmethod
    def init_data_source(self, data_source_config):
        pass


    def check_conn(self):
        return True

    # function to get itemIds from the data source. 
    @abstractmethod
    def get_itemIds(self, item_names: List[str] = [], 
                    host_names: List[str] = [], 
                    group_names: List[str] = []) -> List[int]:
        pass

    # function to get dict of itemId to hostId from the data source.
    @abstractmethod
    def get_items_details(self, itemIds: List[int]) -> pd.DataFrame:
        # ['group_name', 'hostid', 'host_name', 'itemid', 'item_name']
        pass


    # function to get data from the data source. 
    # Returns pandas dataframe with columns: itemid, clock, value
    @abstractmethod
    def get_history_data(self, startep: int, endep: int, itemIds: List[int] = []) -> pd.DataFrame:
        pass

    @abstractmethod
    # get itemid, clock, value
    def get_trends_data(self, startep: int, endep: int, itemIds: List[int] = []) -> pd.DataFrame:
        pass

    @abstractmethod
    # get itemid, clock, value_min, value_avg, value_max
    def get_trends_full_data(self, startep: int, endep: int, itemIds: List[int] = []) -> pd.DataFrame:
        pass
    
    # funtion to classify items by host groups
    def classify_by_groups(self, itemIds: List[int], group_names: List[str]) -> dict:
        return {}
        

    def get_item_detail(self, itemId: int) -> Dict:
        data = self.get_items_details([itemId])
        if data.empty:
            return {}
        data = data.iloc[0]
        return {
            "itemid": data["itemid"],
            "group_name": data["group_name"],
            "hostid": data["hostid"],
            "host_name": data["host_name"],
            "item_name": data["item_name"]
        }
    
    def get_item_html_title(self, itemId: int) -> str:
        data = self.get_items_details([itemId])
        if data.empty:
            return ""
        data = data.iloc[0]
        return f"""{itemId}<br>
                    {data[int(itemId)]['host_name'][:20]}<br>
                    {data[int(itemId)]['item_name'][:20]}
    """