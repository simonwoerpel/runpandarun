import os
import unittest

import pandas as pd

from runpandarun import Datastore


class Test(unittest.TestCase):
    def setUp(self):
        self.store = Datastore.from_yaml('./example/config.yml')
        self.dataset = self.store.datasets[0]

    def test_filesystem_publish(self):
        ds = self.dataset
        fp = ds.publish()
        self.assertIn(self.store.config.publish['public_root'], fp)
        self.assertTrue(os.path.isfile(fp))
        df = pd.read_csv(fp, index_col='id')
        self.assertTrue(df.equals(ds.df))

        # overwrite
        self.assertRaises(FileExistsError, ds.publish)
        fp = ds.publish(overwrite=True)
        self.assertIn(self.store.config.publish['public_root'], fp)

        # with source
        ds.publish(overwrite=True, include_source=True)
        fp = os.path.join(self.store.config.publish['public_root'], ds.name, 'source.csv')
        self.assertTrue(os.path.isfile(fp))
        with open(fp) as f:
            source_content = f.read()
        self.assertEqual(ds._storage.get_source(), source_content)

        # different name and wrangled df
        df = ds.df.T
        fp = ds.publish(df, name='transformed')
        self.assertTrue(os.path.isfile(fp))
        self.assertIn('transformed', fp)
        _df = pd.read_csv(fp, index_col=0)
        self.assertTrue(df.shape == _df.shape)
