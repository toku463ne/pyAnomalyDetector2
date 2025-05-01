from typing import List, Tuple

from db.postgresql import PostgreSqlDB
import utils.config_loader as config_loader

class Model:
    name = ""
    sql_template = ""

    def __init__(self, data_source_name=""):
        self.schema_name = config_loader.conf["admdb"]["schema"]
        self.data_source_name = data_source_name
        if data_source_name == "":
            self.table_name = self.name
        else:
            self.table_name = f"{data_source_name}_{self.name}"
        self.db = PostgreSqlDB(config_loader.conf['admdb'])
        self.batch_size = config_loader.conf["batch_size"]
        self.create_table()

    def truncate(self):
        self.db.exec_sql(f"TRUNCATE TABLE {self.table_name};")
        
    def drop(self):
        self.db.exec_sql(f"DROP TABLE IF EXISTS {self.table_name};")
    
    def create_table(self):
        table_name = f"{self.schema_name}.{self.table_name}"
        if self.db.table_exists(table_name):
            return
        self.db.create_table(table_name, self.sql_template)
        

    def initialize(self):
        self.drop()
        self.create_table()
    
    def check_conn(self) -> bool:
        ok = self.db.table_exists(self.table_name, self.schema_name)
        if ok == False:
            print(f"model {self.schema_name}.{self.table_name} does not exist")
        return ok
    
    def separate_existing_itemIds(self, itemIds: List[int]) -> Tuple[List[int],List[int]]:
        sql = f"SELECT distinct itemid FROM {self.table_name}"
        where_conds = ""
        if len(itemIds) > 0:
            where_conds = f"itemid IN ({','.join(map(str, itemIds))})"

        if where_conds != "":
            sql += " WHERE " + where_conds

        cur = self.db.exec_sql(sql)
        existing = []
        for (itemId,) in cur:
            existing.append(itemId)

        nonexisting = [item for item in itemIds if item not in existing]
        return existing, nonexisting
    
    def count(self) -> int:
        sql = f"SELECT COUNT(*) FROM {self.table_name}"
        (count,) = self.db.exec_sql(sql).fetchone()
        return count
    