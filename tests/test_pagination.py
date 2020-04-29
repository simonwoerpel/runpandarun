import unittest


from runpandarun import Datastore


class Test(unittest.TestCase):
    # base fetching logic see `tests.test_datastore`
    # FIXME this test might fail over time bc. the remote dataset might change / removed at one point

    def setUp(self):
        self.store = Datastore('./example/rki_json.yml')
        self.dataset = self.store.rki_json

    def test_load_paginated(self):
        ds = self.dataset
        # FIXME improve test? for now: just make sure nothing breaks
        df = ds.get_df()  # noqa

    def test_paginate_by_offset(self):
        ds = self.dataset
        ds.update()
        raw_files = ds._storage.backend.get_children(f'{ds.name}/data')
        self.assertGreater(len(raw_files), 1)
        # just make sure nothing breaks
        df = ds.get_df()  # noqa
