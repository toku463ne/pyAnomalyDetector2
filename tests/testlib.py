from typing import Dict, List
from data_processing.detector import Detector
from models.models_set import ModelsSet
import trends_stats

def setup_testdir(testname):
    """
    prepare a test directory
    """
    import os
    import shutil
    import __init__

    if testname == None or testname == "":
        raise Exception("testname is required")

    testdir = os.path.join("/tmp", "anomdec_tests", testname)
    if os.path.exists(testdir):
        shutil.rmtree(testdir)
    os.makedirs(testdir)

    return testdir

def import_test_data(conf: Dict, itemIds: List[int], endep: int):
    """
    Import model data for testing.
    """
    data_sources = conf['data_sources']
    history_interval = conf['history_interval']
    history_retention = conf['history_retention']
    startep = endep - history_interval * history_retention
    trends_stats.update_stats(conf, startep, 0, itemIds=itemIds, initialize=True)
    for data_source_name in data_sources:
        #ModelsSet(data_source_name).initialize()
        d = Detector(data_source_name, conf['data_sources'][data_source_name], itemIds)
        d.initialize_data()
        d.update_history(endep)
        d.update_history_stats(endep)
        d.update_anomalies(endep)
