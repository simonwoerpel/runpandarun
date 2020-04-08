import banal
import os
import unittest
import pandas as pd
from datetime import datetime

from runpandarun import Datastore


class Test(unittest.TestCase):
    def setUp(self):
        self.store = Datastore('./example/config.yml')
        self.config = self.store.config.publish['handlers']['filesystem']
        self.dataset = self.store.datasets[0]

    def test_filesystem_publish(self):
        ds = self.dataset
        fp = ds.publish()[0]
        self.assertIn(self.config['data_root'], fp)
        self.assertTrue(os.path.isfile(fp))
        df = pd.read_csv(fp, index_col='id')
        self.assertTrue(df.equals(ds.get_df()))

        # overwrite
        self.assertRaises(FileExistsError, ds.publish)
        fp = ds.publish(overwrite=True)[0]
        self.assertIn(self.config['data_root'], fp)

        # with source
        ds.publish(overwrite=True, include_source=True)
        fp = os.path.join(self.config['data_root'], ds.name, 'source.csv')
        self.assertTrue(os.path.isfile(fp))
        with open(fp) as f:
            source_content = f.read()
        self.assertEqual(ds._storage.get_source(), source_content)

        # different name and wrangled df
        df = ds.get_df().T
        fp = ds.publish(df, name='transformed')[0]
        self.assertTrue(os.path.isfile(fp))
        self.assertIn('transformed', fp)
        _df = pd.read_csv(fp, index_col=0)
        self.assertTrue(df.shape == _df.shape)

    def test_disabled_handler(self):
        os.environ['FILESYSTEM_PUBLISH_ENABLED'] = '0'
        os.environ['CONFIG'] = './example/config.yml'
        store = Datastore()
        self.assertFalse(banal.as_bool(store.config.publish['handlers']['filesystem']['enabled']))
        res = store.datasets[0].publish()
        self.assertListEqual([], res)
        # re-enable for further tests
        os.environ['FILESYSTEM_PUBLISH_ENABLED'] = 'true'

    def test_publish_with_timestamp(self):
        config = self.store.config
        config['publish']['handlers']['filesystem']['with_timestamp'] = True
        store = Datastore(config)
        fp = store.datasets[0].publish(name='test_with_ts')[0]
        date = datetime.now().date().isoformat()
        self.assertIn(date, fp)
