import pandas as pd
from typing import List, Dict

from models.model import Model
import utils.normalizer as normalizer

class HistoryModel(Model):
    sql_template = "history"
    name = "history"
    fields = ['itemid', 'clock', 'value']

    def get_data(self, itemIds: List[int]=[], startep: int = 0, endep: int = 0) -> pd.DataFrame:
        sql = f"SELECT * FROM {self.table_name}"
        where = []
        if len(itemIds) > 0:
            where.append(f"itemid IN ({','.join(map(str, itemIds))})")
        if startep > 0:
            where.append(f"clock >= {startep}")
        if endep > 0:
            where.append(f"clock <= {endep}")
        if len(where) > 0:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY itemid, clock;"
        
        df = self.db.read_sql(sql)
        if df.empty:
            return pd.DataFrame(columns=self.fields, dtype=object)
        df.columns = self.fields
        return df


    def insert(self, itemids: List[int], clocks: List[int], values: List[float]):
        # prepare sql
        sql = f"INSERT INTO {self.table_name} (itemid, clock, value) VALUES "
        for i in range(len(itemids)):
            sql += f"({itemids[i]}, {clocks[i]}, {values[i]}),"
        sql = sql[:-1] + ";"

        self.db.exec_sql(sql)

    # buggy
    def upsert(self, itemids: List[int], clocks: List[int], values: List[float]):
        # prepare sql
        sql = f"INSERT INTO {self.table_name} (itemid, clock, value) VALUES "
        for i in range(len(itemids)):
            sql += f"({itemids[i]}, {clocks[i]}, {values[i]}),"
        sql = sql[:-1] + " ON CONFLICT (itemid, clock) DO UPDATE SET value = EXCLUDED.value;"

        self.db.exec_sql(sql)
        
    def remove_old_data(self, clock: int):
        sql = f"DELETE FROM {self.table_name} WHERE clock < {clock};"
        self.db.exec_sql(sql)        
        

    def import_history(self, hist_df: pd.DataFrame, base_clocks: List[int]):
        itemids = hist_df['itemid'].tolist()
        
        for itemid in itemids:
            idx = hist_df[hist_df['itemid'] == itemid].index
            clocks = hist_df.loc[idx, 'clock'].tolist()
            values = hist_df.loc[idx, 'value'].tolist()
            values = normalizer.fit_to_base_clocks(base_clocks, clocks, values)
            self.upsert([itemid]*len(base_clocks), base_clocks, values)

    def remove_itemIds_not_in(self, itemIds: List[int]):
        sql = f"DELETE FROM {self.table_name} WHERE itemid NOT IN ({','.join(map(str, itemIds))});"
        self.db.exec_sql(sql)

    def get_charts(self, itemIds: List[int], startep: int, endep: int) -> Dict[int, pd.Series]:
        sql = f"SELECT itemid, clock, value FROM {self.table_name} WHERE itemid IN ({','.join(map(str, itemIds))}) AND clock >= {startep} AND clock <= {endep} ORDER BY itemid, clock;"
        df = self.db.read_sql(sql)
        if df.empty:
            return {}
        
        df.columns = self.fields
        
        charts = {}
        for _, row in df.iterrows():
            itemId = row.itemid
            if itemId not in charts:
                charts[itemId] = []
            charts[itemId].append(row["value"])
        
        # convert to series
        for itemId in charts:
            charts[itemId] = pd.Series(charts[itemId])
        
        return charts