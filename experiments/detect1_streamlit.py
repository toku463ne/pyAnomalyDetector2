import __init__ # # noqa: F401
import detect_anomalies
import utils.config_loader as config_loader
from views.streamlit_view import StreamlitView
from models.models_set import ModelsSet
import trends_stats

"""
To run this script, use the following command:
# streamlit run /home/ubuntu/git/pyAnomalyDetector2/experiments/detect1_streamlit.py
"""

endep = 1739505598 
conf = config_loader.conf
conf["data_sources"] = {
    "csv_datasource": {
        "type": "csv",
        "data_dir": "testdata/csv/20250214_1100"
    },
}
view_source = conf["view_sources"]["flask_view"]
view_source["port"] = 5200
itemIds = [59888, 93281, 94003, 110309, 141917, 217822, 236160, 217825, 270793, 270797, 217823]

trends_stats.update_stats(conf, endep - 3600*24*3, 0, itemIds=itemIds, initialize=True)


detect_anomalies.run(conf=conf, endep=endep,
    itemIds=itemIds,
    initialize=True,
    detection_stages=[1])

ms = ModelsSet("csv_datasource")
itemIds = ms.anomalies.get_itemids()
print(f"Anomalies: {itemIds}")

v = StreamlitView(conf, view_source, data_sources=conf["data_sources"])
v.run()


