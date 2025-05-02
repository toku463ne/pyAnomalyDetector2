import pandas as pd
from typing import List, Dict

from models.model import Model

class AnomaliesModel(Model):
    sql_template = "anomalies"
    name = sql_template
    fields = ["itemid", "created", "group_name", "hostid", "clusterid", "host_name", "item_name", "trend_mean", "trend_std"]

    def get_data(self, where_conds: List[str] = []) -> pd.DataFrame:
        sql = f"SELECT * FROM {self.table_name}"
        if len(where_conds) > 0:
            sql += " WHERE " + " AND ".join(where_conds)
        
        df = self.db.read_sql(sql)
        if df.empty:
            return pd.DataFrame(columns=self.fields, dtype=object)
        df.columns = self.fields
        return df
    
    def get_itemids(self) -> List[int]:
        sql = f"SELECT distinct itemid FROM {self.table_name};"
        cur = self.db.exec_sql(sql)
        itemIds = []
        for (itemId,) in cur:
            itemIds.append(itemId)
        return itemIds
    

    def get_charts(self, itemIds: List[int] = []) -> Dict[int, pd.Series]:
        if len(itemIds) == 0:
            itemIds = self.get_itemids()
        sql = f"SELECT itemid, created, hostid, clusterid, group_name, host_name, item_name, trend_mean, trend_std FROM {self.table_name} WHERE itemid in (%s);" % ",".join(map(str, itemIds))
        df = self.db.read_sql(sql)
        if df.empty:
            return {}
        
        charts = {}
        for _, row in df.iterrows():
            itemId = row.itemid
            if itemId not in charts:
                charts[itemId] = []
            charts[itemId].append(row)
        
        # convert to series
        for itemId in charts:
            charts[itemId] = pd.Series(charts[itemId])
        
        return charts  

    def get_last_updated(self) -> float:
        sql = f"SELECT max(created) FROM {self.table_name}"
        (epoch,) = self.db.select1rec(sql)
        return epoch
    


    def insert_data(self, data: pd.DataFrame):
        for _, row in data.iterrows():
            item_name = row.item_name.replace("'", "")
            trend_mean = 0 if pd.isna(row.trend_mean) else row.trend_mean
            trend_std = 0 if pd.isna(row.trend_std) else row.trend_std
            sql = f"""INSERT INTO {self.table_name} 
    (itemid, created, hostid, clusterid, group_name, host_name, item_name, trend_mean, trend_std) 
    VALUES 
    ({row.itemid}, {row.created}, 
     {row.hostid}, {row.clusterid}, 
     '{row.group_name[:255]}', '{row.host_name[:255]}', '{item_name[:255]}', {trend_mean}, 
     {trend_std})
    ON CONFLICT (itemid, created, group_name) DO UPDATE SET
        hostid = EXCLUDED.hostid,
        clusterid = EXCLUDED.clusterid,
        host_name = EXCLUDED.host_name,
        item_name = EXCLUDED.item_name,
        trend_mean = EXCLUDED.trend_mean,
        trend_std = EXCLUDED.trend_std
    """
            self.db.exec_sql(sql)

    def update_clusterid(self, clusters: Dict):
        for itemId, clusterId in clusters.items():
            sql = f"update {self.table_name} set clusterid = {clusterId} where itemid = {itemId};"
            self.db.exec_sql(sql)

    def delete_old_entries(self, oldep: int):
        sql = f"delete from {self.table_name} WHERE created < {oldep};"
        self.db.exec_sql(sql)

    
    def filter_itemIds(self, itemIds: List[int], created: int):
        sql = f"select itemid from {self.table_name} where created >= {created} and itemid in (%s);" % ",".join(map(str, itemIds))
        cur = self.db.exec_sql(sql)
        ex_itemIds = []
        for (itemId,) in cur:
            ex_itemIds.append(itemId)
        
        # exclude ex_itemIds from itemIds
        itemIds = [itemId for itemId in itemIds if itemId not in ex_itemIds]

        return itemIds

    
    def get_stats_per_itemId(self, itemIds: List[int] = []) -> Dict[int, Dict[str, float]]:
        if len(itemIds) == 0:
            itemIds = self.get_itemids()
        sql = f"SELECT itemid, trend_mean, trend_std FROM {self.table_name} WHERE itemid in (%s);" % ",".join(map(str, itemIds))
        df = self.db.read_sql(sql)
        if df.empty:
            return {}
        
        stats = {}
        for _, row in df.iterrows():
            stats[row.itemid] = {"mean": row.trend_mean, "std": row.trend_std}
        
        return stats
    

    def import_data(self, csv_file: str, itemIds: List[int] = []):
        df = pd.read_csv(csv_file)
        df.columns = self.fields
        if len(itemIds) > 0:
            df = df[df.itemid.isin(itemIds)]
        self.insert_data(df)

    