from typing import Dict

from models.models_set import ModelsSet
import utils.config_loader as config_loader
import data_getter

def report(conf: Dict, epoch: int) -> Dict:

    anomaly_cache_ep = epoch - conf['anomaly_keep_secs']
    data = {}
    data_sources = conf["data_sources"]
    for data_source_name in data_sources:
        ms = ModelsSet(data_source_name)
        # get anomaly dataframe
        df = ms.anomalies.get_data([f"created >= {anomaly_cache_ep}",
                                    f"created <= {epoch}"])
        if df.empty:
            data[data_source_name] = {}
            continue
        
        last_created = df["created"].max()
        df = df[df["created"] == last_created]
        df = df[df["clusterid"] != -1]
        
        df = df.groupby(["clusterid", "hostid"]).first().reset_index()

        # group by clusterid and get the count per clusterid
        cnt = df.groupby("clusterid")["clusterid"].count()
        # get clusterids with count > 1
        clusterids = cnt[cnt > 1].index
        # filter df by clusterids
        df = df[df["clusterid"].isin(clusterids)]

        data[data_source_name] = {}
        for clusterid in clusterids:
            data[data_source_name][clusterid] = {}
            for _, row in df[df["clusterid"] == clusterid].iterrows():
                group_name = row["group_name"]
                host_name = row["host_name"]
                if group_name not in data[data_source_name][clusterid]:
                    data[data_source_name][clusterid][group_name] = {}
                if host_name not in data[data_source_name][clusterid][group_name]:
                    data[data_source_name][clusterid][group_name][host_name] = []
                data[data_source_name][clusterid][group_name][host_name].append(f'{row["itemid"]}: {row["item_name"]}')
 
    return data

if __name__ == "__main__":
    # read arguments
    import argparse
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-c', '--config', type=str, help='config yaml file')
    parser.add_argument('--end', type=int, default=0, help='End epoch.')
    parser.add_argument('--output', type=str, default="", help='Output file.')

    # suppress python warnings
    import warnings
    warnings.filterwarnings("ignore")

    args = parser.parse_args()
    config = config_loader.load_config(args.config)
    
    data = report(config, args.end)
    
    data["has_anomalies"] = "no"
    if len(data) > 0:
        for k, v in data.items():
            if k == "has_anomalies":
                continue
            if len(v) > 0:
                data["has_anomalies"] = "yes"
                break
    
    import json
    if args.output:
        with open(args.output, "w") as f:
            f.write(json.dumps(data, indent=4))
    else:
        print(json.dumps(data, indent=4))
    
