from typing import Dict
from flask import Flask, render_template, request, jsonify
import plotly.graph_objs as go
import plotly.io as pio
from typing import List
import pandas as pd

import __init__
from view.view import View
from models.models_set import ModelsSet
import data_getter

class FlaskView(View):
    def __init__(self, config: Dict, data_sources: Dict) -> None:
        self.app = Flask(__name__)
        self.config = config
        layout = config["layout"]
        self.max_vertical_charts = layout.get("max_vertical_charts", 5)
        self.max_horizontal_charts = layout.get("max_horizontal_charts", 3)
        self.max_charts = self.max_vertical_charts * self.max_horizontal_charts
        self.chart_width = layout.get("chart_width", 800)
        self.chart_height = layout.get("chart_height", 600)
        self.chart_type = layout.get("chart_type", "line")
        self.itemIds = layout.get("itemIds", [])
        self.group_names = layout.get("group_names", [])
        self.data_sources = data_sources
        


    # generate chart from metric data
    # df: DataFrame with columns ['itemid', 'clock', 'value']
    def _generate_charts_in_group(self, df: pd.DataFrame, itemIds: List[int], group_name: str) -> str:
        charts = []
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

    def generate_charts(self) -> Dict:
        for data_source in self.data_sources.values():
            dg = data_getter.get_data_getter(data_source)




    def show(self) -> None:
        @self.app.route("/")
        def index():
            return render_template("index.html")

        @self.app.route("/api/data", methods=["GET"])
        def get_data():
            data = {"message": "Hello, World!"}
            return jsonify(data)