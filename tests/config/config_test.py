import unittest

import delta.config.config_manager as c


class TestConfigManager(unittest.TestCase):
    def test_configParse(self):
        cm = c.ConfigManager()
        cm.parserAppConfig()
        hdfsURL = cm.getAppConfigBy('hadoop', 'dfs', 'url')
        self.assertEqual(hdfsURL, 'hdfs://localhost:9000')


if __name__ == '__main__':
    unittest.main()
