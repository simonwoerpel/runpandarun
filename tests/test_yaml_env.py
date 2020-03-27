import os
import unittest

from runpandarun.config import Config


class Test(unittest.TestCase):
    def test_env(self):
        config = """
        api_key: !ENV ${API_KEY}
        data_root: !ENV '${DATA_ROOT}/test'
        """

        os.environ['API_KEY'] = 'a secret key'
        os.environ['DATA_ROOT'] = '/tmp'

        config = Config(config)

        self.assertEqual('a secret key', config.api_key)
        self.assertEqual('/tmp/test', config.data_root)
