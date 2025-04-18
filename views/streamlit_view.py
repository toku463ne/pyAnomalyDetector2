from typing import Dict
import streamlit as st
import plotly.graph_objs as go
from plotly.subplots import make_subplots
from typing import List, Dict
import pandas as pd
import os

import __init__
from views.view import View
from models.models_set import ModelsSet
import data_getter
import utils

CAT_BY_GROUP = "bygroup"
CAT_BY_CLUSTER = "bycluster"
CAT_LATEST = "latest"
default_chart_categories = {
    "bygroup": "By Group",
    "bycluster": "By Cluster",
    "latest": "Latest"
}

class StreamlitView(View):
    def __init__(self, config: Dict, view_source: Dict, data_sources: Dict) -> None:
        self.trends_interval = config["trends_interval"]
        self.trends_retention = config["trends_retention"]
        self.history_interval = config["history_interval"]
        self.history_retention = config["history_retention"]

        self.view_source = view_source
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
        self.chart_width = layout.get("chart_width", 600)
        self.chart_height = layout.get("chart_height", 300)
        self.chart_type = layout.get("chart_type", "line")
        self.data_sources = data_sources

    def _generate_charts_in_group(self, df: pd.DataFrame, properties: Dict) -> go.Figure:
        itemIds = df['itemid'].unique()
        n_items = len(itemIds)
        n_cols = min(self.max_horizontal_charts, n_items)
        n_rows = (n_items + n_cols - 1) // n_cols

        fig = make_subplots(
            rows=n_rows,
            cols=n_cols,
            subplot_titles=[
                f"{itemId}<br>{properties[itemId]['host_name'][:20]}<br>{properties[itemId]['item_name'][:20]}"
                for itemId in itemIds
            ]
        )

        fig.update_layout(
            hovermode="x unified",
        )
        for axis in fig.layout:
            if axis.startswith('xaxis') or axis.startswith('yaxis'):
                fig.layout[axis].update(
                    showspikes=True,
                    spikemode="across",
                    spikesnap="cursor",
                    spikedash="dot",
                    matches="x" if axis.startswith('xaxis') else None
                )

        for i, itemId in enumerate(itemIds):
            item_df = df[df['itemid'] == itemId]
            item_df['clock'] = pd.to_datetime(item_df['clock'], unit='s').dt.strftime("%m-%d %H")
            row = (i // n_cols) + 1
            col = (i % n_cols) + 1

            # Decide number of digits based on data range
            y_values = item_df['value']
                      
            digits = utils.get_float_format(y_values, 4)
            fig.add_trace(
                go.Scatter(
                    x=item_df['clock'],
                    y=y_values.apply(lambda x: float(f"{x:.{digits}g}")),
                    mode="lines",
                    name=f"{properties[itemId]['item_name']}"
                ),
                row=row,
                col=col
            )

        fig.update_layout(
            height=max(self.chart_height * n_rows + 100, 400),
            width=max(self.chart_width * n_cols + 200, 900),
            autosize=True,
            margin=dict(l=20, r=20, t=20, b=20),
            showlegend=False,
        )
        return fig

    def _generate_charts_by_group(self) -> Dict[str, go.Figure]:
        opts = self.chart_categories[CAT_BY_GROUP]
        charts = {}
        for data_source_name, data_source in self.data_sources.items():
            ms = ModelsSet(data_source_name)
            if len(self.itemIds) > 0:
                data = ms.anomalies.get_data([f"itemid in ({','.join(map(str, self.itemIds))})"])
            else:
                data = ms.anomalies.get_data()

            endep = data["created"].max()
            trend_start = endep - self.trends_retention * self.trends_interval
            history_start = endep - self.history_retention * self.history_interval
            history_end = endep

            pdata = data[["group_name", "host_name", "item_name", "itemid"]]
            pdata = pdata.drop_duplicates(subset=["group_name", "host_name", "item_name", "itemid"])
            properties = pdata.set_index("itemid").T.to_dict()

            if opts.get("one_item_per_host", True):
                data = data.groupby(["group_name", "hostid", "clusterid", "itemid"]).agg({"itemid": "min"}).reset_index()
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
                    charts[group_name] = pd.concat([charts[group_name], df])
                else:
                    charts[group_name] = df

        charts_fig = {}
        for group_name in charts:
            itemIds = charts[group_name]['itemid'].unique()
            if len(itemIds) > self.max_charts:
                for i in range(0, len(itemIds), self.max_charts):
                    sub_group_name = f"{group_name}_{i}"
                    itemIds_block = itemIds[i:i + self.max_charts]
                    df_block = charts[group_name][charts[group_name]['itemid'].isin(itemIds_block)]
                    charts_fig[sub_group_name] = self._generate_charts_in_group(df_block, properties)
            else:
                charts_fig[group_name] = self._generate_charts_in_group(charts[group_name], properties)
        return charts_fig

    def run(self) -> None:
        st.title("Anomaly Detector Charts")

        categoryid = st.radio(
            "Select Category",
            options=list(self.chart_categories.keys()),
            format_func=lambda k: self.chart_categories[k]["name"],
            horizontal=True
        )

        if categoryid == CAT_BY_GROUP:
            charts_by_group = self._generate_charts_by_group()
            for group_name, fig in charts_by_group.items():
                st.subheader(group_name)
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning(f"Category '{categoryid}' is not supported in Streamlit view.")

