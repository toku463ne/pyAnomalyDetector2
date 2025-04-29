#!/bin/bash
# time scripts/run_view.sh -c samples/zabbix.yml 
source $HOME/venv/bin/activate
if [ "$ANOMDEC_SECRET_PATH" == "" ]; then
    export ANOMDEC_SECRET_PATH="$HOME/.creds/anomdec.yaml"
fi

echo "$(date) python3 viewer.py $@"
date;time nice python3 viewer.py $@
