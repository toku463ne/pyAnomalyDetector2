

from models.history import HistoryModel
from models.anomalies import AnomaliesModel
from db.postgresql import PostgreSqlDB
import utils.config_loader as config_loader


class ModelsSet:
    def __init__(self, data_source_name):
        self.data_source_name = data_source_name
        self.create_schema()
        self.create_tables(data_source_name)

    # create schema with name of data_source_name 
    def create_schema(self):
        db = PostgreSqlDB(config_loader.conf["admdb"])
        db.create_schema(self.data_source_name)

    
    def create_tables(self, data_source_name):
        self.history = HistoryModel(data_source_name)
        self.anomalies = AnomaliesModel(data_source_name)        

    def drop(self):
        self.history.drop()
        self.anomalies.drop()

    def initialize(self):
        self.drop()
        self.create_tables()

    def check_conn(self) -> bool:
        for m in [self.history, self.anomalies]:
            if not m.check_conn():
                return False
        return True
    