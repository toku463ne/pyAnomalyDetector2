"""
Update the zabbix_dashboard view
"""
import pandas as pd
from pyzabbix import ZabbixAPI
import logging

from models.models_set import ModelsSet
import utils.config_loader as config_loader
from views.view import View

def log(msg, level=logging.INFO):
    msg = f"[views/zabbix.py] {msg}"
    logging.log(level, msg)

class ZabbixDashboard(View):
    def __init__(self, config):
        self.dashboard_name = config["dashboard_name"]
        zapi = ZabbixAPI(config["api_url"])
        zapi.login(config["user"], config["password"])
        self.zapi = zapi
        self.config = config


    # show dashboard
    def update(self, data: pd.DataFrame):
        log("show dashboard")
        if len(data) == 0:
            return
        # get min(itemid) for each group_name, hostid
        data = data.groupby(["group_name", "hostid", "clusterid"]).agg({"itemid": "min"}).reset_index()
        data = data.sort_values(by=["group_name", "hostid"])
        data = data[["group_name", "itemid"]]
        data = data.drop_duplicates(subset=["group_name", "itemid"])

        pagedata = {}
        for _, row in data.iterrows():
            group_name = row["group_name"]
            itemid = row["itemid"]
            if group_name not in pagedata:
                pagedata[group_name] = []
            pagedata[group_name].append(itemid)
        
        self.create_dashboard(self.dashboard_name, pagedata)
        log("completed")

    def update_latest(self, data: pd.DataFrame):
        log("show latest dashboard")
        if len(data) == 0:
            return
        last_ep = data["created"].max()
        data = data[data["created"] == last_ep]
        data = data.sort_values(by=["clusterid", "hostid", "itemid"])
        data = data[data["clusterid"] != 1]
        
        pagedata = {}
        for _, row in data.iterrows():
            clusterid = row["clusterid"]
            if clusterid not in pagedata:
                pagedata[clusterid] = []
            pagedata[clusterid].append(row["itemid"])
        
        dashboard_name = f"{self.dashboard_name}_latest"
        self.create_dashboard(dashboard_name, pagedata)
        log("completed")

    def update_cluster(self, data: pd.DataFrame):
        log("show by cluster dashboard")
        if len(data) == 0:
            return

        data = data.sort_values(by=["clusterid", "hostid", "itemid"])
        data = data.drop_duplicates(subset=["clusterid", "hostid", "itemid"])
        data_multi = data[data.groupby("clusterid")["clusterid"].transform("count") > 1]
        data_single = data[data.groupby("clusterid")["clusterid"].transform("count") == 1]
        data_single.loc[:, "clusterid"] = -1
        data = pd.concat([data_single, data_multi])
        
        pagedata = {}
        for _, row in data.iterrows():
            clusterid = row["clusterid"]
            if clusterid not in pagedata:
                if len(pagedata) >= 50:
                    break
                pagedata[clusterid] = []
            pagedata[clusterid].append(row["itemid"])
        
        dashboard_name = f"{self.dashboard_name}_bycluster"
        self.create_dashboard(dashboard_name, pagedata)
        log("completed")

            
    def check_conn(self):
        version = self.zapi.api_version()
        if version == None:
            return False
        return True
    
    # get dashboard
    def get_dashboard(self, dashboard_name):
        zapi = self.zapi
        d = zapi.dashboard.get(filter={"name": dashboard_name}, selectPages="extend")
        if len(d) == 0:
            return None
        else:
            return d[0]

    # delete dashboard
    def delete_dashboard(self, dashboard_name):
        zapi = self.zapi
        d = self.get_dashboard(dashboard_name)
        if d is None:
            return
        zapi.dashboard.delete(dashboardid=d["dashboardid"])

    # create dashboard
    # pagedata = [(page_name, [itemid1, itemid2, ...]), ...]
    def create_dashboard(self, dashboard_name, pagedata, ncols=4, nrows=12, vsize=5, hsize=15):
        if pagedata == None or len(pagedata) == 0:
            return
        pages = []
        for (name, itemids) in pagedata.items():
            if len(itemids) > 0:
                n = ncols * nrows
                i = 0
                while True:
                    if n*i >= len(itemids):
                        break
                    zitemids=itemids[n*i:(i+1)*n]
                    i += 1

                    x = 0
                    y = 0
                    widgets = []
                    for itemid in zitemids:
                        widget = {'type':'graph',
                                'x':x*hsize,'y':y*vsize,
                                'width':hsize,'height':vsize,
                                'view_mode':'0',
                                'fields':[
                                    {'type':'0','name':'source_type','value':'1'},
                                    {'type':'4','name':'itemid','value':int(itemid)}]
                                }
                        widgets.append(widget)
                        x += 1
                        if x % ncols == 0:
                            x = 0
                            y += 1
                    
                    pages.append({'name': '%s_%s' % (name, str(i)), 'widgets': widgets})

        if self.get_dashboard(dashboard_name) == None:
            self._create_dashboard(dashboard_name, pages)
        else:
            self._update_dashboard(dashboard_name, pages)


    def _create_dashboard(self, dashboard_name, pages):
        zapi = self.zapi
        #log(pages)
        zapi.dashboard.create(name=dashboard_name, pages=pages)


    def _update_dashboard(self, dashboard_name, pages):
        zapi = self.zapi
        d =     self.get_dashboard(dashboard_name)
        zapi.dashboard.update(dashboardid=d["dashboardid"], pages=pages)



