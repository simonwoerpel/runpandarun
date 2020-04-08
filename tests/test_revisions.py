import os
import unittest
from datetime import datetime

from runpandarun.revisions import DatasetRevisions
from runpandarun.store import Datastore


class Test(unittest.TestCase):
    def setUp(self):
        store = Datastore('./example/config.yml')
        self.ds = store.datasets[0]

    def _fp(self, name):
        return os.path.join(self.ds.revisions._backend.get_base_path(), 'revisions', f'{name}.pkl')

    def test_revisions(self):
        ds = self.ds
        self.assertIsInstance(ds.revisions, DatasetRevisions)
        ds.revisions.save('transformed', ds.get_df().T)
        self.assertIn('transformed', ds.revisions)
        self.assertIn('transformed', ds.revisions.list())
        self.assertTrue(os.path.isfile(self._fp('transformed')))

        self.assertTrue(ds['transformed'].equals(ds.revisions['transformed']))
        rev = ds['transformed']
        self.assertTrue(rev.equals(ds.get_df().T))

        # store other stuff (anything that pickle can handle)
        now = datetime.now()
        foo = {'bar': now}
        ds.revisions.save('foo', foo)
        # retrieve
        foo = ds['foo']
        self.assertIn('foo', ds.revisions)
        self.assertEqual(foo['bar'], now)
        self.assertEqual(ds['foo']['bar'], now)
