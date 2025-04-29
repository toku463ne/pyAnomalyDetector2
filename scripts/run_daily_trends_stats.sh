#!/bin/bash
# time scripts/run_daily_trends_stats.sh -c samples/zabbix.yml --init
source $HOME/venv/bin/activate
if [ "$ANOMDEC_SECRET_PATH" == "" ]; then
    export ANOMDEC_SECRET_PATH="$HOME/.creds/anomdec.yaml"
fi
python3 trends_stats.py $@ 