#!/bin/bash
# time scripts/run_daily_trends_stats.sh -c samples/zabbix.yml --init
source $HOME/venv/bin/activate
export ANOMDEC_SECRET_PATH="$HOME/.creds/zabbix_api.yaml"
python3 trends_stats.py $@ 
