import pandas as pd
from typing import List

from models.model import Model

class StatsModel(Model):
    """ fields:
        itemid: INT
        sum: FLOAT
        sqr_sum: FLOAT
        end: INT
        mean: FLOAT
        std: FLOAT
    """
    sql_template = "stats"
    name = sql_template
    fields = ['itemid', 'sum', 'sqr_sum', 'cnt', 'mean', 'std']


    
    def upsert_stats(self, itemid: int, sum: float, sqr_sum: float, cnt: int, 
                     mean: float, std: float):
        # prepare sql
        sql = f"INSERT INTO {self.table_name} (itemid, sum, sqr_sum, cnt, mean, std) VALUES "
        sql += f"({itemid}, {sum}, {sqr_sum}, {cnt}, {mean}, {std}) "
        sql += " ON CONFLICT (itemid) DO UPDATE SET "
        sql += "sum = EXCLUDED.sum, sqr_sum = EXCLUDED.sqr_sum, cnt = EXCLUDED.cnt, "
        sql += "mean = EXCLUDED.mean, "
        sql += "std = EXCLUDED.std;"
        sql = sql[:-1] + ";"

        self.db.exec_sql(sql)
        
    def read_stats(self, itemids: List[int] = []) -> pd.DataFrame:
        sql = f"SELECT * FROM {self.table_name}"
        where = []
        if len(itemids) > 0:
            where.append(f"itemid IN ({','.join(map(str, itemids))})")
        if len(where) > 0:
            sql += " WHERE " + " AND ".join(where)

        df = self.db.read_sql(sql)
        if len(df) == 0:
            return pd.DataFrame(columns=self.fields, dtype=object)
        df.columns = self.fields
        return df

    def get_stats_per_itemId(self, itemIds: List[int] = []) -> dict:
        stats = {}
        df = self.read_stats(itemIds)
        for _, row in df.iterrows():
            itemId = row.itemid
            stats[itemId] = {
                'cnt': int(row["cnt"]),
                'mean': float(row["mean"]),
                'std': float(row["std"])
            }
        return stats