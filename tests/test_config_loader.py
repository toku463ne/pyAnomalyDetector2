import unittest, os

import __init__
import tests.testlib as testlib
import utils.config_loader as config_loader

class TestConfigLoader(unittest.TestCase):
    # test load_config
    def test_env(self):
        os.environ['ANOMDEC_SECRET_PATH'] = os.path.join('tests', 'test_secret.yml')

        conf = config_loader.load_config()
        self.assertIsNotNone(conf)
        self.assertIn('logging', conf)

        l = conf["logging"]
        self.assertEqual(l["log_dir"], os.path.join(os.environ["HOME"], "anomdec/logs"))

        admdb = conf["admdb"]
        self.assertEqual(admdb["host"], "localhost")
        self.assertEqual(admdb["user"], "anomdec")
        self.assertEqual(admdb["password"], "anomdec_pass")
        
if __name__ == '__main__':
    unittest.main()
