from models.model import Model
from datetime import datetime

class UpdatesModel(Model):
    sql_template = "updates"
    name = "updates"


    
    def upsert_updates(self, startep, endep):
        self.db.exec_sql(f"""INSERT INTO {self.table_name} (endep, enddt, startep)
    VALUES({endep}, '{datetime.fromtimestamp(endep)}', {startep})
    ON CONFLICT(endep) DO UPDATE SET
    enddt=excluded.enddt;""")
        
    
    def get_endep(self):
        val = self.db.select1value(self.table_name, "endep", 
                                   [f'endep = (select max(endep) from {self.table_name})'])
        if val is None:
            return 0
        return val
    
    def get_startep(self):
        val = self.db.select1value(self.table_name, "startep", 
                                   [f'endep = (select max(endep) from {self.table_name})'])
        if val is None:
            return 0
        return val