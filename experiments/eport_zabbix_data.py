import os, sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import tools.get_zabbix_data as zabbix_data
import utils.config_loader as config_loader
os.environ['ANOMDEC_SECRET_PATH'] = os.path.join(os.environ["HOME"], ".creds/zabbix_api.yaml")

config = config_loader.load_config("samples/zabbix.yml")

exporter = zabbix_data.ZabbixDataExporter(
    config=config,
    output_dir=os.path.join("/tmp"),
    history_length=600 * 3,
    trends_length=3600
)

exporter.export_data_from_anomalies()

