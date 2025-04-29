#!/bin/bash
# time scripts/run_viewer.sh -c samples/zabbix.yml 
source $HOME/venv/bin/activate
if [ "$ANOMDEC_SECRET_PATH" == "" ]; then
    export ANOMDEC_SECRET_PATH="$HOME/.creds/anomdec.yaml"
fi

echo "$(date) python3 update_views.py $@"
date;time nice python3 update_views.py $@
