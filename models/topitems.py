from models.anomalies import AnomaliesModel

class TopItemsModel(AnomaliesModel):
    sql_template = "anomalies"
    name = "topitems"
    
    