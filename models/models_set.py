from models.history import HistoryModel
from models.history_stats import HistoryStatsModel
from models.history_updates import HistoryUpdatesModel
from models.trends_stats import TrendsStatsModel
from models.trends_updates import TrendsUpdatesModel
from models.anomalies import AnomaliesModel
from models.topitems import TopItemsModel
from db.postgresql import PostgreSqlDB
import utils.config_loader as config_loader


class ModelsSet:
    def __init__(self, data_source_name):
        self.data_source_name = data_source_name
        self.schema_name = config_loader.conf["admdb"]["schema"]
        self.create_schema()
        self.load_models()

    # create schema with name of data_source_name 
    def create_schema(self):
        db = PostgreSqlDB(config_loader.conf["admdb"])
        db.create_schema(self.schema_name)

    
    def load_models(self):
        self.history = HistoryModel(self.data_source_name)
        self.history_updates = HistoryUpdatesModel(self.data_source_name)
        self.history_stats = HistoryStatsModel(self.data_source_name)
        self.trends_stats = TrendsStatsModel(self.data_source_name)
        self.trends_updates = TrendsUpdatesModel(self.data_source_name)
        self.anomalies = AnomaliesModel(self.data_source_name)
        self.topitems = TopItemsModel(self.data_source_name)
        
        self.models = [
            self.history,
            self.history_updates,
            self.history_stats,
            self.trends_stats,
            self.trends_updates,
            self.anomalies,
            self.topitems
        ]
        

    def drop(self):
        for m in self.models:
            try:
                m.drop()
            except Exception as e:
                print(f"Error dropping model {m.__class__.__name__}: {e}")
        

    def initialize(self):
        self.drop()
        self.load_models()

    def check_conn(self) -> bool:
        for m in self.models:
            try:
                if not m.check_conn():
                    return False
            except Exception as e:
                print(f"Error checking connection for model {m.__class__.__name__}: {e}")
        return True

