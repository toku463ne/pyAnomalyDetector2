#!/bin/bash
# time scripts/export_zabbix_data.sh -c samples/zabbix.yml -o /tmp/anomdec/$(date +"%Y%m%d")

source $HOME/venv/bin/activate
export ANOMDEC_SECRET_PATH="$HOME/.creds/zabbix_api.yaml"
date;time nice python3 tools/get_zabbix_data.py $@


