import __init__ # # noqa: F401
import utils.config_loader as config_loader
from views.streamlit_view import StreamlitView
from models.models_set import ModelsSet
import tests.testlib as testlib
import classifiers.dbscan as dbscan

"""
anomaly detection
"""


# To run this script, use the following command:
# streamlit run /home/ubuntu/git/pyAnomalyDetector2/experiments/dbscan_streamlit.py

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
view_source["chart_categories"] = {"bycluster": {
        "name": "By Cluster",
        "one_item_per_host": False}
}
itemIds = [59888, 93281, 94003, 110309, 141917, 217822, 236160, 217825, 270793, 270797, 217823]

testlib.import_test_data(conf, itemIds, endep)
clusters, centroids, chart_info = dbscan.classify_charts(
    conf,
    itemIds=itemIds,
    endep=endep,
)
for data_source_name in conf["data_sources"]:
    ms = ModelsSet(data_source_name)
    ms.anomalies.update_clusterid(clusters)


v = StreamlitView(conf, view_source, data_sources=conf["data_sources"])
v.run()


