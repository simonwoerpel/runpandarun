import os
import unittest

from runpandarun.revisions import DatasetRevisions
from runpandarun.store import Datastore


class Test(unittest.TestCase):
    def setUp(self):
        store = Datastore('./example/config.yml')
        self.ds = store.datasets[0]

    def _fp(self, name):
        return os.path.join(self.ds.revisions._data_root, f'{name}.csv')

    def test_revisions(self):
        ds = self.ds
        self.assertIsInstance(ds.revisions, DatasetRevisions)
        df = ds.df
        ds.save(ds.df.T, 'transformed')
        self.assertIn('transformed', ds.revisions)
        self.assertIn('transformed', ds.revisions.show())
        self.assertTrue(os.path.isfile(self._fp('transformed')))

        self.assertTrue(ds['transformed'].equals(ds.revisions['transformed']))
        rev = ds['transformed']
        self.assertEqual(df.T.shape, rev.shape)
        self.assertListEqual(list(df.columns), list(rev.T.columns))
