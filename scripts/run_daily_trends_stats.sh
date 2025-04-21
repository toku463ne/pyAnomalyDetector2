#!/bin/bash
# time scripts/run_trends_stats.sh -c tests/test_zabbix.d/config.yml --init
source $HOME/venv/bin/activate
export SECRET_PATH="$HOME/.creds/zabbix_api.yaml"
python3 trends_stats.py $@ 
