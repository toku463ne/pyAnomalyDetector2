#!/bin/bash
# time scripts/export_zabbix_data.sh -c samples/zabbix.yml -o /tmp/anomdec/$(date +"%Y%m%d")

source $HOME/venv/bin/activate
if [ "$ANOMDEC_SECRET_PATH" == "" ]; then
    export ANOMDEC_SECRET_PATH="$HOME/.creds/anomdec.yaml"
fi
date;time nice python3 tools/get_zabbix_data.py $@


