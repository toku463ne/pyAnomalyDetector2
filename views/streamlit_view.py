import os
from typing import Dict
import streamlit as st
import plotly.graph_objs as go
from plotly.subplots import make_subplots
from typing import List, Dict
import pandas as pd
import os

#import __init__
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
radio_order = [CAT_BY_GROUP, CAT_BY_CLUSTER, CAT_LATEST]

class StreamlitView(View):
    def __init__(self, config: Dict, view_source: Dict) -> None:
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
        self.n_sigma = view_source.get("n_sigma", 3)
        
        layout = view_source["layout"]
        self.max_vertical_charts = layout.get("max_vertical_charts", 5)
        self.max_horizontal_charts = layout.get("max_horizontal_charts", 3)
        self.max_charts = self.max_vertical_charts * self.max_horizontal_charts
        self.chart_width = layout.get("chart_width", 600)
        self.chart_height = layout.get("chart_height", 300)
        self.chart_type = layout.get("chart_type", "line")
        self.data_sources = config["data_sources"]

    def _generate_charts_in_group(self, df: pd.DataFrame, properties: Dict, titles: Dict) -> go.Figure:
        itemIds = df['itemid'].unique()
        n_items = len(itemIds)
        n_cols = min(self.max_horizontal_charts, n_items)
        n_rows = (n_items + n_cols - 1) // n_cols

        fig = make_subplots(
            rows=n_rows,
            cols=n_cols,
            subplot_titles=[
            titles.get(int(itemId), f"""{itemId}<br>
                    {properties[int(itemId)]['host_name'][:20]}<br>
                    {properties[int(itemId)]['item_name'][:20]}<br>""")
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
            item_df = df[df['itemid'] == itemId].copy()
            # sort by clock (ensure ascending order)
            item_df = item_df.sort_values(by='clock')
            # convert clock to datetime (keep as datetime, don't format as string)
            item_df['clock'] = pd.to_datetime(item_df['clock'], unit='s')
            row = (i // n_cols) + 1
            col = (i % n_cols) + 1
            itemId = int(itemId)

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
            plus_n_sigma = mean + self.n_sigma * std
            minus_n_sigma = mean - self.n_sigma * std
            y_min, y_max = y_values.min(), y_values.max()
            x_vals = item_df['clock']

            if y_min - std <= plus_n_sigma <= y_max + std:
                fig.add_trace(
                    go.Scatter(
                        x=x_vals,
                        y=[plus_n_sigma] * len(x_vals),
                        mode="lines",
                        line=dict(dash='dash', color='red'),
                        name=f"+{self.n_sigma}σ"
                    ),
                    row=row,
                    col=col
                )
            if y_min - std <= minus_n_sigma <= y_max + std:
                fig.add_trace(
                    go.Scatter(
                        x=x_vals,
                        y=[minus_n_sigma] * len(x_vals),
                        mode="lines",
                        line=dict(dash='dash', color='blue'),
                        name=f"-{self.n_sigma}σ"
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
            height=max(self.chart_height * n_rows + 100, 500),
            width=max(self.chart_width * n_cols + 200, 900),
            autosize=True,
            margin=dict(l=20, r=20, t=80, b=20),
            showlegend=False,
        )
        return fig

    def _generate_charts_by_category(self, categoryid: str, selected_chart_type: str) -> Dict[str, go.Figure]:
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
        titles = {}
        properties = {}
        for data_source_name, data_source in self.data_sources.items():
            ms = ModelsSet(data_source_name)
            if selected_chart_type == "topitems":
                m = ms.topitems
            else:
                m = ms.anomalies
            if len(self.itemIds) > 0:
                data = m.get_data([f"itemid in ({','.join(map(str, self.itemIds))})"])
            else:
                data = m.get_data()

            endep = data["created"].max()
            trend_start = endep - self.trends_retention * self.trends_interval
            history_start = endep - self.history_retention * self.history_interval
            history_end = endep

            pdata = data[prop_keys]
            pdata = pdata.sort_values("created").drop_duplicates(subset=prop_keys[:-3], keep="last")
            properties.update(pdata.set_index("itemid").T.to_dict())

            if opts.get("one_item_per_host", True):
                groupby_keys_no_itemid = [k for k in groupby_keys if k != "itemid"]
                data = data.groupby(groupby_keys_no_itemid).agg({"itemid": "min"}).reset_index()
            data = data.sort_values(by=sort_keys)
            data = data[drop_keys]
            data = data.drop_duplicates(subset=drop_keys)
            group_names = data[group_key].unique()

            for group_name in group_names:
                group_data = data[data[group_key] == group_name]
                itemIds_block = group_data["itemid"].tolist()
                if len(itemIds_block) == 0:
                    continue

                chart_group_name = f"{data_source_name}/{group_name}"

                dg = data_getter.get_data_getter(data_source)
                t_df = dg.get_trends_data(trend_start, history_start, itemIds_block)
                h_df = dg.get_history_data(history_start, history_end, itemIds_block)
                df = pd.concat([t_df, h_df])
                titles_block = {}
                for itemId in itemIds_block:
                    titles_block[itemId] = dg.get_item_html_title(itemId, selected_chart_type)
                if chart_group_name in charts:
                    charts[chart_group_name] = pd.concat([charts[chart_group_name], df])
                    titles_block.update(titles[chart_group_name])
                else:
                    charts[chart_group_name] = df
                    titles[chart_group_name] = titles_block

        charts_fig = {}
        for group_name in charts:
            itemIds = charts[group_name]['itemid'].unique()
            round = 0
            for i in range(0, len(itemIds), self.max_charts):
                sub_group_name = f"{group_name}_{round}"
                itemIds_block = itemIds[i:i + self.max_charts]
                df_block = charts[group_name][charts[group_name]['itemid'].isin(itemIds_block)]
                charts_fig[sub_group_name] = self._generate_charts_in_group(df_block, properties, titles[group_name])
                round += 1
        return charts_fig

    def _generate_charts_by_group(self, selected_chart_type) -> Dict[str, go.Figure]:
        return self._generate_charts_by_category(CAT_BY_GROUP, selected_chart_type)

    def _generate_charts_by_cluster(self, selected_chart_type) -> Dict[str, go.Figure]:
        return self._generate_charts_by_category(CAT_BY_CLUSTER, selected_chart_type)


    def show_charts(self) -> None:

        # list box in the top right corner to switch between anomaly/trends
        st.sidebar.title("chart type")
        st.sidebar.selectbox(
            "Select Chart Type",
            options=["anomalies", "topitems"],
            index=0,
            key="chart_type"
        )
        
        selected_chart_type = st.session_state.get("chart_type", "anomalies")

        if selected_chart_type == "topitems":
            st.title("Top Items Charts")
        else:
            st.title("Anomaly Detector Charts")

        options = list(self.chart_categories.keys())
        # sort by radio_order
        options = sorted(options, key=lambda x: radio_order.index(x) if x in radio_order else len(radio_order))
        categoryid = st.radio(
            "Select Grouping Method",
            options=options,
            format_func=lambda k: self.chart_categories[k]["name"],
            horizontal=True
        )


        if categoryid in [CAT_BY_GROUP, CAT_BY_CLUSTER]:
            if categoryid == CAT_BY_GROUP:
                charts = self._generate_charts_by_group(selected_chart_type)
            elif categoryid == CAT_BY_CLUSTER:
                charts = self._generate_charts_by_cluster(selected_chart_type)
            if charts:
                tab_names = [str(name) for name in charts.keys()]
                tabs = st.tabs(tab_names)
                for tab, group_name in zip(tabs, tab_names):
                    with tab:
                        st.plotly_chart(charts[group_name], use_container_width=True, key=f"plotly_chart_{group_name}")
        else:
            st.warning(f"Category '{categoryid}' is not supported in Streamlit view.")


    def show_item_details(self, itemid: int, chart_type: str) -> None:
        st.title(f"Details for Item ID: {itemid}")

        # Find the data source containing this itemid
        found = False
        for data_source_name, data_source in self.data_sources.items():
            if chart_type == "topitems":
                m = ModelsSet(data_source_name).topitems
            else:
                m = ModelsSet(data_source_name).anomalies

            data = m.get_data([f"itemid = {itemid}"])
            if not data.empty:
                found = True
                break

        if not found:
            st.error(f"No data found for itemid {itemid}")
            return

        # Show item properties
        dg = data_getter.get_data_getter(data_source)
        st.subheader("Item Details")
        st.json(dg.get_item_detail(itemid))

        # Get time ranges
        endep = data["created"].max()
        trend_start = endep - self.trends_retention * self.trends_interval
        history_start = endep - self.history_retention * self.history_interval
        history_end = endep

        # Get time series data
        t_df = dg.get_trends_data(trend_start, history_start, [itemid])
        h_df = dg.get_history_data(history_start, history_end, [itemid])
        df = pd.concat([t_df, h_df])
        df = df.sort_values(by='clock')
        df['clock'] = pd.to_datetime(df['clock'], unit='s')

        # Plot chart
        st.subheader("Time Series Chart")
        digits = utils.get_float_format(df['value'], 4)
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=df['clock'],
                y=df['value'].apply(lambda x: float(f"{x:.{digits}g}")),
                mode="lines",
                name="value"
            )
        )
        # Get item properties for the current itemid
        item_props = data.loc[data['itemid'] == itemid].iloc[0]
        mean = item_props['trend_mean']
        std = item_props['trend_std']
        plus_n_sigma = mean + self.detect1_lambda_threshold * std
        minus_n_sigma = mean - self.detect1_lambda_threshold * std
        x_vals = df['clock']

        fig.add_trace(
            go.Scatter(
                x=x_vals,
                y=[mean] * len(x_vals),
                mode="lines",
                line=dict(dash='dot', color='green'),
                name="mean"
            )
        )
        fig.add_trace(
            go.Scatter(
                x=x_vals,
                y=[plus_n_sigma] * len(x_vals),
                mode="lines",
                line=dict(dash='dash', color='red'),
                name="+3σ"
            )
        )
        fig.add_trace(
            go.Scatter(
                x=x_vals,
                y=[minus_n_sigma] * len(x_vals),
                mode="lines",
                line=dict(dash='dash', color='blue'),
                name="-3σ"
            )
        )
        fig.update_layout(
            hovermode="x unified",
            height=500,
            width=900,
            margin=dict(l=20, r=20, t=70, b=20),
            showlegend=True,
        )
        st.plotly_chart(fig, use_container_width=True, key=f"details_plotly_chart_{itemid}")


def run(config: Dict) -> None:
    query_params = st.query_params
    print(f"Query Params: {query_params}")
    #print(f"Config: {config}")
    st.set_page_config(layout="wide")

    view_source_name = query_params.get("view_source", "")
    if view_source_name == "":
        for view_source_name, view_source in config["view_sources"].items():
            if view_source["type"] == "streamlit":
                break
    else:
        view_source = config["view_sources"][view_source_name]
    
    v = StreamlitView(config, view_source)

    page = query_params.get("page", "")    
    chart_type = query_params.get("chart_type", "")
    if page == "details":
        itemid = int(query_params.get("itemid", 0))
        if itemid > 0:
            v.show_item_details(itemid, chart_type)
        else:
            st.error("Invalid itemid")
    else:
        v.show_charts()



        
