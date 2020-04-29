import unittest


from runpandarun import Datastore


class Test(unittest.TestCase):
    # base fetching logic see `tests.test_datastore`

    def setUp(self):
        self.store = Datastore('./example/config.yml')
        self.dataset = self.store.datasets[0]

    def test_dedup_update_by_hash(self):
        ds = self.dataset
        ds.update()
        raw_files = ds._storage.backend.get_children(f'{ds.name}/data')

        # update again will not store the source, bc it didn't change:
        ds.update()
        self.assertListEqual(ds._storage.backend.get_children(f'{ds.name}/data'), raw_files)

    def test_url_replace(self):
        # FIXME improve tests
        ds = self.store.divi_reports
        ds.update()
