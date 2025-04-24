#!/bin/bash
# time scripts/run_view.sh -c samples/zabbix.yml 
source $HOME/venv/bin/activate
export ANOMDEC_SECRET_PATH="$HOME/.creds/zabbix_api.yaml"

echo "$(date) python3 viewer.py $@"
date;time nice python3 viewer.py $@
