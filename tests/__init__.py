import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
os.environ['ANOMDEC_SECRET_PATH'] = os.path.join('tests', 'test_secret.yml')
import utils.config_loader as config_loader
config_loader.load_config(os.path.join('tests', 'test_config.yml'))