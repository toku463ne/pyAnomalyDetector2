#!/bin/bash
# time scripts/run_streamlit.sh -c samples/streamlit.yml 
script_folder=$(dirname "$(realpath "$0")")
source $HOME/venv/bin/activate
if [ "$ANOMDEC_SECRET_PATH" == "" ]; then
    export ANOMDEC_SECRET_PATH="$HOME/.creds/anomdec.yaml"
fi

export PYTHONPATH=$(pwd)
echo date;time nice streamlit run streamlit_server.py -- $@
date;time nice streamlit run streamlit_server.py -- $@