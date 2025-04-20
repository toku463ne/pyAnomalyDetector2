from typing import Dict
import streamlit as st
st.set_page_config(layout="wide")
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
        self.detect1_lambda_threshold = config["detect1_lambda_threshold"]

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

            mean = properties[itemId]['trend_mean']
            std = properties[itemId]['trend_std']

            # Add +3σ and -3σ lines if within y range
            plus_3sigma = mean + self.detect1_lambda_threshold * std
            minus_3sigma = mean - self.detect1_lambda_threshold * std
            y_min, y_max = y_values.min(), y_values.max()
            x_vals = item_df['clock']

            if y_min - std <= plus_3sigma <= y_max + std:
                fig.add_trace(
                    go.Scatter(
                        x=x_vals,
                        y=[plus_3sigma] * len(x_vals),
                        mode="lines",
                        line=dict(dash='dash', color='red'),
                        name="+3σ"
                    ),
                    row=row,
                    col=col
                )
            if y_min - std <= minus_3sigma <= y_max + std:
                fig.add_trace(
                    go.Scatter(
                        x=x_vals,
                        y=[minus_3sigma] * len(x_vals),
                        mode="lines",
                        line=dict(dash='dash', color='blue'),
                        name="-3σ"
                    ),
                    row=row,
                    col=col
                )
            # Add mean line
            if y_min - std <= mean <= y_max + std:
                fig.add_trace(
                    go.Scatter(
                        x=x_vals,
                        y=[mean] * len(x_vals),
                        mode="lines",
                        line=dict(dash='dot', color='green'),
                        name="mean"
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

    def _generate_charts_by_category(self, categoryid: str) -> Dict[str, go.Figure]:
        if categoryid == CAT_BY_GROUP:
            group_key = "group_name"
            opts = self.chart_categories[CAT_BY_GROUP]
            prop_keys = ["group_name", "host_name", "item_name", "itemid", "created", "trend_mean", "trend_std"]
            groupby_keys = ["group_name", "hostid", "clusterid", "itemid"]
            sort_keys = ["group_name", "hostid"]
            drop_keys = ["group_name", "itemid"]
        elif categoryid == CAT_BY_CLUSTER:
            group_key = "clusterid"
            opts = self.chart_categories[CAT_BY_CLUSTER]
            prop_keys = ["clusterid", "host_name", "item_name", "itemid", "created", "trend_mean", "trend_std"]
            groupby_keys = ["clusterid", "hostid", "itemid"]
            sort_keys = ["clusterid", "hostid"]
            drop_keys = ["clusterid", "itemid"]
        else:
            raise ValueError(f"Unsupported categoryid: {categoryid}")

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

            pdata = data[prop_keys]
            pdata = pdata.sort_values("created").drop_duplicates(subset=prop_keys[:-3], keep="last")
            properties = pdata.set_index("itemid").T.to_dict()

            if opts.get("one_item_per_host", True):
                data = data.groupby(groupby_keys).agg({"itemid": "min"}).reset_index()
            data = data.sort_values(by=sort_keys)
            data = data[drop_keys]
            data = data.drop_duplicates(subset=drop_keys)
            group_names = data[group_key].unique()

            for group_name in group_names:
                group_data = data[data[group_key] == group_name]
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

    def _generate_charts_by_group(self) -> Dict[str, go.Figure]:
        return self._generate_charts_by_category(CAT_BY_GROUP)

    def _generate_charts_by_cluster(self) -> Dict[str, go.Figure]:
        return self._generate_charts_by_category(CAT_BY_CLUSTER)



    def run(self) -> None:
        st.title("Anomaly Detector Charts")

        categoryid = st.radio(
            "Select Category",
            options=list(self.chart_categories.keys()),
            format_func=lambda k: self.chart_categories[k]["name"],
            horizontal=True
        )

        if categoryid in [CAT_BY_GROUP, CAT_BY_CLUSTER]:
            if categoryid == CAT_BY_GROUP:
                charts = self._generate_charts_by_group()
            elif categoryid == CAT_BY_CLUSTER:
                charts = self._generate_charts_by_cluster()
            if charts:
                tab_names = [str(name) for name in charts.keys()]
                tabs = st.tabs(tab_names)
                for tab, group_name in zip(tabs, tab_names):
                    with tab:
                        st.plotly_chart(charts[int(group_name)], use_container_width=True, key=f"plotly_chart_{group_name}")
        else:
            st.warning(f"Category '{categoryid}' is not supported in Streamlit view.")

