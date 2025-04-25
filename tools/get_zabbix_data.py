from typing import List, Dict
import os

import __init__
import utils.config_loader as config_loader
from data_getter.zabbix_getter import ZabbixGetter
from models.models_set import ModelsSet


class ZabbixDataExporter:
    history_file_name = "history.csv.gz"
    trends_file_name = "trends.csv.gz"
    item_details_file_name = "items.csv.gz"
    anom_data_file_name = "anomalies.csv.gz"

    def __init__(self, config: Dict, output_dir: str, history_length: int, trends_length: int):
        self.history_length = history_length
        self.trends_length = trends_length
        self.output_dir = output_dir
        # ensure the output directory exists
        os.makedirs(self.output_dir, exist_ok=True)

        self.conf = config
        self.data_source_config = {}
        for data_source_name in self.conf["data_sources"]:
            data_source_config = self.conf["data_sources"][data_source_name]
            if data_source_config["type"] == "zabbix":
                self.data_source_config = data_source_config
                self.data_source_name = data_source_name
                break

        self.ms = ModelsSet(self.data_source_name)
        self.z = ZabbixGetter(self.data_source_config)


    def export_data(self, endep: int, itemIds: List[int]):
        print("exporting trends data")
        df = self.z.get_trends_full_data(endep - self.trends_length, endep, itemIds=itemIds)
        # Save the DataFrame to a gzipped CSV file
        file_path = os.path.join(self.output_dir, self.trends_file_name)
        df.to_csv(file_path, mode='w', index=False, compression='gzip')

        print("exporting history data")
        df = self.z.get_history_data(endep - self.history_length, endep, itemIds=itemIds)
        # Save the DataFrame to a gzipped CSV file
        file_path = os.path.join(self.output_dir, self.history_file_name)
        df.to_csv(file_path, mode='w', index=False, compression='gzip')

        print("exporting items details")
        df = self.z.get_items_details(itemIds=itemIds)
        # Save the DataFrame to a gzipped CSV file
        file_path = os.path.join(self.output_dir, self.item_details_file_name)
        df.to_csv(file_path, mode='w', index=False, compression='gzip')

        # write endep to a file
        endep_file_path = os.path.join(self.output_dir, "endep.txt")
        with open(endep_file_path, 'w') as f:
            f.write(str(endep))
        print(f"endep: {endep} written to {endep_file_path}")


    def export_data_from_anomalies(self):
        itemIds = self.ms.anomalies.get_itemids()
        endep = self.ms.anomalies.get_last_updated()
        self.export_data(endep, itemIds)

        anom_data = self.ms.anomalies.get_data()
        # Save the DataFrame to a gzipped CSV file
        file_path = os.path.join(self.output_dir, self.anom_data_file_name)
        anom_data.to_csv(file_path, mode='w', index=False, compression='gzip')
        print(f"anomalies data written to {file_path}")




if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-c', '--config', type=str, help='config yaml file')
    parser.add_argument('-o', '--outdir', type=str, help='output directory')
    parser.add_argument('--history_length', type=int, default=3600 * 24, help='history length in seconds')
    parser.add_argument('--trends_length', type=int, default=3600 * 24 * 14, help='trends length in seconds')
    args = parser.parse_args()
    
    config = config_loader.load_config(args.config)
    output_dir = args.outdir
    history_length = args.history_length
    trends_length = args.trends_length
    zabbix_data_exporter = ZabbixDataExporter(config, output_dir, history_length, trends_length)
    zabbix_data_exporter.export_data_from_anomalies()


    