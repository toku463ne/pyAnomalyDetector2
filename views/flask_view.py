from typing import Dict
from flask import Flask, render_template, request, jsonify
import plotly.graph_objs as go
import plotly.io as pio
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

class FlaskView(View):
    def __init__(self, config: Dict, data_sources: Dict) -> None:
        self.app = Flask(__name__, template_folder=config.get("template_dir", "templates"))
        self.config = config
        self.host = config.get("host", "0.0.0.0")
        self.port = config.get("port", 5000)
        self.debug = config.get("debug", False)
        self.trend_start = config.get("trend_start", 0)
        self.history_start = config.get("history_start", 0)
        self.history_end = config.get("history_end", 0)
        self.itemIds = config.get("itemids", [])
        self.group_names = config.get("group_names", [])
        layout = config["layout"]
        self.max_vertical_charts = layout.get("max_vertical_charts", 5)
        self.max_horizontal_charts = layout.get("max_horizontal_charts", 3)
        self.max_charts = self.max_vertical_charts * self.max_horizontal_charts
        self.chart_width = layout.get("chart_width", 800)
        self.chart_height = layout.get("chart_height", 600)
        self.chart_type = layout.get("chart_type", "line")
        self.data_sources = data_sources
        self.chart_categories = layout.get("chart_categories", [CAT_BY_GROUP])
        

    # generate chart from metric data
    # df: DataFrame with columns ['itemid', 'clock', 'value']
    def _generate_charts_in_group(self, df: pd.DataFrame, group_name: str) -> str:
        charts = []
        itemIds = df['itemid'].unique()
        for i, itemId in enumerate(itemIds):
            item_df = df[df['itemid'] == itemId]
            fig = go.Figure()
            item_df['clock'] = pd.to_datetime(item_df['clock'], unit='s')
            fig.add_trace(go.Scatter(x=item_df['clock'], y=item_df['value'], mode=self.chart_type, name=str(itemId)))

            fig.update_layout(
            title=f"{group_name} - {itemId}",
            xaxis_title="Clock",
            yaxis_title="Value",
            width=self.chart_width,
            height=self.chart_height
            )
            chart_html = pio.to_html(fig, full_html=False)
            charts.append(chart_html)

            # Break into a new row if max_horizontal_charts is reached
            if (i + 1) % self.max_horizontal_charts == 0:
                charts.append("<br>")

            # Stop adding charts if max_vertical_charts is reached
            if (i + 1) // self.max_horizontal_charts >= self.max_vertical_charts:
                break

        return "".join(charts)


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
            data = data.groupby(["group_name", "hostid", "clusterid"]).agg({"itemid": "min"}).reset_index()
            data = data.sort_values(by=["group_name", "hostid"])
            data = data[["group_name", "itemid"]]
            data = data.drop_duplicates(subset=["group_name", "itemid"])
            group_names = data["group_name"].unique()
            for group_name in group_names:
                group_data = data[data["group_name"] == group_name]
                itemIds_block = group_data["itemid"].tolist()
                dg = data_getter.get_data_getter(data_source)
                t_df = dg.get_trends_data(self.trend_start, self.history_start, itemIds_block)
                h_df = dg.get_history_data(self.history_start, self.history_end, itemIds_block)
                df = pd.concat([t_df, h_df])
                if group_name in charts:
                    # concatenate df
                    charts[group_name] = pd.concat([charts[group_name], df])
                else:
                    charts[group_name] = df
        # generate charts by group
        charts_html = {}
        for group_name in charts:
            charts_html[group_name] = self._generate_charts_in_group(charts[group_name], group_name)
        return charts_html


    def _generate_charts(self) -> Dict:
        categories = []
        for categoryid in self.chart_categories:
            if categoryid == CAT_BY_GROUP:
                charts_by_group = self._generate_charts_by_group()
                category = {
                    "id": categoryid,
                    "chart_groups": [{"id": group_name, "charts": charts_html} 
                            for group_name, charts_html in charts_by_group.items()]
                }
            else:
                raise Exception(f"categoryid {categoryid} is not supported")
            
            categories.append(category)

        return categories

    
    def run(self) -> None:
        @self.app.route("/", methods=["GET"])
        def index():
            return render_template("index.html")

        @self.app.route("/charts", methods=["GET"])
        def charts():
            # Regenerate charts every time the /charts endpoint is accessed
            charts = self._generate_charts()
            return render_template("charts.html.j2", categories=charts)

        @self.app.route("/status", methods=["GET"])
        def get_status():
            data = {"status": "OK"}
            return jsonify(data)
        
        self.app.run(
            host=self.host,
            port=self.port,
            debug=self.debug
        )