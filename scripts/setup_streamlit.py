import os
from dotenv import load_dotenv
# load environment variables from .env file
load_dotenv()

# deploy templates/nginx_streamlitserver.conf.j2 to /etc/nginx/sites-available/streamlitserver.conf with context of os.environ
with open('templates/nginx_streamlitserver.conf.j2', 'r') as f:
    nginx_conf = f.read()

nginx_conf = nginx_conf.format(**os.environ)
with open('/etc/nginx/sites-available/streamlitserver.conf', 'w') as f:
    f.write(nginx_conf)

# create a symbolic link to /etc/nginx/sites-enabled/streamlitserver.conf
os.system('ln -s /etc/nginx/sites-available/streamlitserver.conf /etc/nginx/sites-enabled/streamlitserver.conf')

# deploy templates/systemd_streamlit.service.j2 to /etc/systemd/system/streamlit.service
with open('templates/systemd_streamlit.service.j2', 'r') as f:
    systemd_conf = f.read()

systemd_conf = systemd_conf.format(**os.environ)
with open('/etc/systemd/system/streamlit.service', 'w') as f:
    f.write(systemd_conf)

    