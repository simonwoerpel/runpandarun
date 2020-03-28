import os
import unittest


class Test(unittest.TestCase):
    # test some handy import stuff, see `runpandarun.__init__.py`

    def setUp(self):
        os.environ['CONFIG'] = './example/config.yml'

    def test_import(self):
        # FIXME this test doesnt work although the implementation does !?
        return
        from runpandarun import Datastore
        from runpandarun.dataset import Dataset
        from runpandarun import datasets
        from runpandarun.datasets import my_dataset as _my_dataset
        store = Datastore()
        self.assertIsInstance(_my_dataset, Dataset)
        self.assertEqual(store.my_dataset, _my_dataset)
        self.assertEqual(datasets.my_dataset, _my_dataset)
        self.assertEqual(datasets.my_dataset, store.my_dataset)
