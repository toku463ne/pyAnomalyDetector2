from typing import Dict
from flask import Flask, render_template, request, jsonify
import plotly.graph_objs as go
import plotly.io as pio
from plotly.subplots import make_subplots
from typing import List
import pandas as pd
import os

import __init__
from views.view import View
from models.models_set import ModelsSet
import data_getter


CAT_BY_GROUP = "bygroup"
CAT_BY_CLUSTER = "bycluster"
CAT_LATEST = "latest"
default_chart_categories = {
    "bygroup": "By Group",
    "bycluster": "By Cluster",
    "latest": "Latest"
}

class FlaskView(View):
    def __init__(self, config: Dict, view_source: Dict, data_sources: Dict) -> None:
        self.app = Flask(__name__, template_folder=view_source.get("template_dir", "templates"))
        self.trends_interval = config["trends_interval"]
        self.trends_retention = config["trends_retention"]
        self.history_interval = config["history_interval"]
        self.history_retention = config["history_retention"]


        self.view_source = view_source
        self.host = view_source.get("host", "0.0.0.0")
        self.port = view_source.get("port", 5000)
        self.debug = view_source.get("debug", False)
        #self.trend_start = view_source.get("trend_start", 0)
        #self.history_start = view_source.get("history_start", 0)
        #self.history_end = view_source.get("history_end", 0)
        self.itemIds = view_source.get("itemids", [])
        self.group_names = view_source.get("group_names", [])
        self.chart_categories = view_source.get("chart_categories", default_chart_categories)
        self.tmp_dir = view_source.get("tmp_dir", "tmp")
        if not os.path.exists(self.tmp_dir):
            os.makedirs(self.tmp_dir)
        
        layout = view_source["layout"]
        self.max_vertical_charts = layout.get("max_vertical_charts", 5)
        self.max_horizontal_charts = layout.get("max_horizontal_charts", 3)
        self.max_charts = self.max_vertical_charts * self.max_horizontal_charts
        self.chart_width = layout.get("chart_width", 400)
        self.chart_height = layout.get("chart_height", 200)
        self.chart_type = layout.get("chart_type", "line")
        self.data_sources = data_sources
 
    # generate chart from metric data
    # df: DataFrame with columns ['itemid', 'clock', 'value']
    def _generate_charts_in_group(self, df: pd.DataFrame, properties: Dict) -> str: 
        itemIds = df['itemid'].unique()
        fig = make_subplots(
            rows=self.max_vertical_charts, 
            cols=self.max_horizontal_charts, 
            subplot_titles=[
            f"{itemId}<br>{properties[itemId]['host_name'][:30]}<br>{properties[itemId]['item_name'][:30]}" 
            for itemId in itemIds[:self.max_horizontal_charts * self.max_vertical_charts]
            ]
        )

        # Enable hover mode with moving grid lines for all subplots
        fig.update_layout(
            hovermode="x unified",  # Enables a unified hover mode along the x-axis
        )
        for axis in fig.layout:
            if axis.startswith('xaxis') or axis.startswith('yaxis'):
                fig.layout[axis].update(
                    showspikes=True,
                    spikemode="across",
                    spikesnap="cursor",
                    spikedash="dot",
                    matches="x" if axis.startswith('xaxis') else None  # Synchronize panning only across x-axis
                )
            

        for i, itemId in enumerate(itemIds):
            item_df = df[df['itemid'] == itemId]
            item_df['clock'] = pd.to_datetime(item_df['clock'], unit='s').dt.strftime("%m-%d %H")
            row = (i // self.max_horizontal_charts) + 1
            col = (i % self.max_horizontal_charts) + 1


            fig.add_trace(
            go.Scatter(
                x=item_df['clock'], 
                y=item_df['value'].apply(lambda x: f"{x:.4g}"), 
                mode="lines", 
                name=f"{properties[itemId]['item_name']}"
            ),
            row=row, 
            col=col
            )



        fig.update_layout(
            height=self.chart_height * self.max_vertical_charts,
            width=self.chart_width * self.max_horizontal_charts,
            showlegend=False,
        )

        chart_html = pio.to_html(fig, full_html=False)
        return chart_html


    def _generate_charts_by_group(self) -> Dict[str, str]:
        """
        charts by group
        """
        charts = {}
        for data_source_name, data_source in self.data_sources.items():
            ms = ModelsSet(data_source_name)
            if len(self.itemIds) > 0:
                data = ms.anomalies.get_data([f"itemid in ({','.join(map(str, self.itemIds))})"])
            else:
                data = ms.anomalies.get_data()

            # max of created
            endep = data["created"].max()
            trend_start = endep - self.trends_retention * self.trends_interval
            history_start = endep - self.history_retention * self.history_interval
            history_end = endep

            # get properties group_name, host_name, item_name from data
            pdata = data[["group_name", "host_name", "item_name", "itemid"]]
            # remove duplicates
            pdata = pdata.drop_duplicates(subset=["group_name", "host_name", "item_name", "itemid"])
            # convert to dict
            properties = pdata.set_index("itemid").T.to_dict()

            data = data.groupby(["group_name", "hostid", "clusterid"]).agg({"itemid": "min"}).reset_index()
            data = data.sort_values(by=["group_name", "hostid"])
            data = data[["group_name", "itemid"]]
            data = data.drop_duplicates(subset=["group_name", "itemid"])
            group_names = data["group_name"].unique()

            for group_name in group_names:
                group_data = data[data["group_name"] == group_name]
                itemIds_block = group_data["itemid"].tolist()
                if len(itemIds_block) == 0:
                    continue

                dg = data_getter.get_data_getter(data_source)
                t_df = dg.get_trends_data(trend_start, history_start, itemIds_block)
                h_df = dg.get_history_data(history_start, history_end, itemIds_block)
                df = pd.concat([t_df, h_df])
                if group_name in charts:
                    # concatenate df
                    charts[group_name] = pd.concat([charts[group_name], df])
                else:
                    charts[group_name] = df
        # generate charts by group
        charts_html = {}
        for group_name in charts:
            # if number of charts in a group is greater than max_charts, devide into multiple groups
            itemIds = charts[group_name]['itemid'].unique()
            if len(itemIds) > self.max_charts:
                for i in range(0, len(itemIds), self.max_charts):
                    sub_group_name = f"{group_name}_{i}"
                    itemIds_block = itemIds[i:i + self.max_charts]
                    df_block = charts[group_name][charts[group_name]['itemid'].isin(itemIds_block)]
                    charts_html[sub_group_name] = self._generate_charts_in_group(df_block, properties)
            else:
                charts_html[group_name] = self._generate_charts_in_group(charts[group_name], properties)
        return charts_html


    def _generate_charts(self, categoryid) -> Dict:
        chart_groups = {}        
        if categoryid == CAT_BY_GROUP:
            charts_by_group = self._generate_charts_by_group()
            i = 0
            for group_name, charts_html in charts_by_group.items():
                id = f"chart_{i}"
                if i == 0:
                    active = "show active"
                    link_active = "active"
                    selected = "true"
                else:
                    active = ""
                    link_active = ""
                    selected = "false"
                chart_groups[id] = {
                    "group_name": group_name,
                    "charts_html": charts_html,
                    "active": active,
                    "link_active": link_active,
                    "selected": selected
                }
                i += 1
        else:
            raise Exception(f"categoryid {categoryid} is not supported")
            

        with self.app.app_context():
            htmltxt = render_template("charts.html.j2", chart_groups=chart_groups, categories=self.chart_categories, selected_category=categoryid)
        if self.debug:
            # save html to file
            with open(os.path.join(self.tmp_dir, "charts.html"), "w") as f:
                f.write(htmltxt)
        return htmltxt

    
    def run(self) -> None:
        @self.app.route("/", methods=["GET"])
        def index():
            return render_template("index.html")

        @self.app.route("/charts", methods=["GET"])
        def charts():
            categoryid = request.args.get("category", CAT_BY_GROUP)
            if categoryid not in self.chart_categories:
                return jsonify({"error": f"Invalid category: {categoryid}"}), 400
            return self._generate_charts(categoryid)

        @self.app.route("/status", methods=["GET"])
        def get_status():
            data = {"status": "OK"}
            return jsonify(data)
        

        self.app.run(
            host=self.host,
            port=self.port,
            debug=self.debug
        )