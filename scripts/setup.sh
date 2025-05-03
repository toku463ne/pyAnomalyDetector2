#!/bin/bash

envfile=$1
if [ "$envfile" == "" ]; then
    envfile="scripts/setup.env"
fi
source $envfile

script_folder=$(dirname "$(realpath "$0")")
#cd $script_folder

set -eu

# Update package lists
apt-get update

# Install Python3 and pip
apt-get install -y python3 python3-pip python3-apt python3-distutils python3-setuptools python3-virtualenv
# Install additional dependencies
apt-get install -y build-essential libssl-dev libffi-dev python3-dev libxml2-dev libxslt1-dev zlib1g-dev

# Create a virtual environment
virtualenv $HOME/venv

# Activate the virtual environment
source $HOME/venv/bin/activate

# Install required Python packages
echo "current dir: $(pwd)"
echo pip install -r requirements.txt
pip install -r requirements.txt

# register the streamlit service
python3 tools/render_template.py templates/systemd_streamlit.service.j2 /etc/systemd/system/streamlit.service

systemctl daemon-reload
systemctl enable streamlit.service
systemctl start streamlit.service

echo "Setup complete. Virtual environment created and dependencies installed."
