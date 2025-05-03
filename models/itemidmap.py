import pandas as pd
from typing import List
from models.model import Model

class ItemIdMapModel(Model):
    sql_template = "itemidmap"
    name = "itemid_map"
    fields = ["itemid", "hostid", "org_itemid", "updated"]

    def get_data(self, itemIds: List[int] = []) -> pd.DataFrame:
        sql = f"SELECT * FROM {self.table_name}"
        where = []
        if len(itemIds) > 0:
            where.append(f"itemid IN ({','.join(map(str, itemIds))})")
        if len(where) > 0:
            sql += " WHERE " + " AND ".join(where)
        
        df = self.db.read_sql(sql)
        if df.empty:
            return pd.DataFrame(columns=self.fields, dtype=object)
        df.columns = self.fields
        return df
    
    def get_max_itemId(self) -> int:
        sql = f"SELECT max(itemid) FROM {self.table_name}"
        cur = self.db.exec_sql(sql)
        for (itemId,) in cur:
            return itemId
        return 0
    
    def get_min_itemId(self) -> int:
        sql = f"SELECT min(itemid) FROM {self.table_name}"
        cur = self.db.exec_sql(sql)
        for (itemId,) in cur:
            return itemId
        return 0
    
    def upsert(self, itemids: List[int], hostids: List[int], org_itemids: List[str], updated: int):
        # prepare sql
        sql = f"INSERT INTO {self.table_name} (itemid, hostid, org_itemid, updated) VALUES "
        for i in range(len(itemids)):
            sql += f"({itemids[i]}, {hostids[i]}, '{org_itemids[i]}', {updated}),"
        sql = sql[:-1] + " ON CONFLICT (itemid) DO UPDATE SET hostid = excluded.hostid, org_itemid = excluded.org_itemid, updated = excluded.updated;"
        
        self.db.exec_sql(sql)

    def delete_old_data(self, updated: int):
        sql = f"DELETE FROM {self.table_name} WHERE updated < {updated}"
        self.db.exec_sql(sql)

