import os
import yaml
from jinja2 import Template
import logging
from typing import Dict

SECRET_PATH = 'ANOMDEC_SECRET_PATH'


BASE_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")
SQL_DIR = os.path.join(BASE_DIR, "db/sql")

conf = {}

def load_config(config_path=None, additional_context={}) -> Dict:
    global conf
    global log_file

    with open('default.yml', 'r') as file:
        conf = yaml.safe_load(file)

    if config_path:
        with open(config_path, 'r') as file:
            override_config = yaml.safe_load(file)
        for key, value in override_config.items():
            if isinstance(value, dict) and key in conf:
                conf[key].update(value)
            else:
                conf[key] = value

    # if os env var SECRET_PATH is set, use that
    if SECRET_PATH in os.environ:
        secret_path = os.environ[SECRET_PATH]
    else:
        secret_path = conf.get('secret_path', None)
        secrets = {}

    if secret_path:
        with open(secret_path, 'r') as file:
            secrets = yaml.safe_load(file)
    
    # os environment variables
    context = {}
    for k in os.environ:
        context[k] = os.environ[k]

    context.update(secrets)
    context.update(additional_context)

    conf = yaml.safe_load(Template(yaml.dump(conf)).render(context))

    logging_enabled = False
    l = conf.get("logging", None)
    if l:
        logging_enabled = l.get("enabled", False)
        if logging_enabled:
            log_dir = l.get("log_dir", os.path.join(context["HOME"], "anomdec/logs"))
            # ensure log dir exists
            os.makedirs(log_dir, exist_ok=True)
            log_file = l.get("file", "app.log")
            log_file = os.path.join(log_dir, log_file)

            # setup logging
            logging.basicConfig(
                filename=log_file,
                level=getattr(logging, l.get("level", "INFO").upper(), logging.INFO),
                format=l.get("format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            )
            logging.info(f"Logging to {log_file}")
    if not logging_enabled:
        logging.info("Logging is disabled. Logging to stdout.")
        # log to stdout
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

    return conf




