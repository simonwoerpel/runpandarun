import json
import unittest

from runpandarun import Datastore


class Test(unittest.TestCase):
    def setUp(self):
        self.store = Datastore('./example/rki.yml')
        self.dataset = self.store.rki
        with open('./example/rki.json') as f:
            self.data = json.load(f)

    def test_json(self):
        ds = self.dataset
        data = self.data
        self.assertDictEqual(data, json.loads(ds._storage.get_source()))
        self.assertEqual(len(data['features']), len(ds.df))

    def test_remote_json(self):
        # TODO this test will fail sooner or later...
        store = Datastore('./example/rki_remote.yml')
        ds = store.rki
        self.assertEqual(len(self.data['features']), len(ds.df))
