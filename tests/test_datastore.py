import unittest

import pandas as pd

from runpandarun.dataset import Dataset, RESAMPLE_METHODS, RESAMPLE_INTERVALS
from runpandarun.store import Datastore
from runpandarun.storage import Storage


class Test(unittest.TestCase):
    def setUp(self):
        self.store = Datastore('./example/config.yml')

    def test_init(self):
        store = self.store
        self.assertIn('datastore-testdata', repr(store))
        self.assertIn('datastore-testdata', repr(store._storage))
        self.assertIn('datastore-testdata', store._storage.backend.get_base_path())

    def test_store(self):
        store = self.store
        self.assertIsInstance(store._storage, Storage)

        # updating
        store.update()
        self.assertIsNotNone(store.last_update)
        self.assertIsNotNone(store.last_complete_update)
        last_complete_update = store.last_complete_update
        store.update()
        self.assertGreater(store.last_complete_update, last_complete_update)

    def test_store_datasets(self):
        store = self.store
        self.assertIsInstance(store.datasets, list)
        self.assertIsInstance([d for d in store], list)
        self.assertEqual(3, len(store.datasets))
        dataset = store.datasets[0]
        self.assertEqual(getattr(store, 'my_dataset'), dataset)
        dataset = store.datasets[1]
        self.assertEqual(getattr(store, 'a_local_csv'), dataset)

    def test_datasets(self):
        dataset = self.store.datasets[0]
        self.assertIsInstance(dataset, Dataset)

    def test_df(self):
        df = self.store.datasets[0].get_df()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual('id', df.index.name)

    def test_json(self):
        ds = self.store.a_local_json
        self.assertTrue(ds.config.dt_index)
        df = ds.get_df()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual('date', df.index.name)

    def test_dtindex(self):
        df = self.store.a_local_csv.get_df()
        self.assertIsInstance(df.index, pd.DatetimeIndex)

    def test_resampling(self):
        ds = self.store.a_local_csv
        for interval in RESAMPLE_INTERVALS.keys():
            resample = getattr(ds, interval, None)
            self.assertIsNotNone(resample)
            for method in RESAMPLE_METHODS.keys():
                func = getattr(resample, method, None)
                self.assertIsNotNone(func)
                self.assertTrue(callable(func))
                if interval == 'yearly':
                    df = func()
                    self.assertIsInstance(df, pd.DataFrame)
                    self.assertEqual(len(df), len(df.index.year.unique()))
                    if method == 'count':
                        self.assertEqual(df.shape[1], 1)
                        self.assertEqual(list(df.columns), ['count'])

    def test_combine_long(self):
        df1 = self.store.a_local_csv.get_df()
        df2 = self.store.a_local_json.get_df()
        combined = self.store.combined
        self.assertSetEqual(set(combined.columns), set(df1.columns))
        self.assertEqual(len(df1) + len(df2), len(combined))
        self.assertTrue(combined.equals(pd.concat([df1, df2]).sort_index()))

    def test_combine_wide(self):
        # add a dummy (copied) dataset
        config = """
        storage:
          filesystem:
            data_root: datastore-testdata/test_combine
        combine:
          - a_local_csv
          - same_but_different
        datasets:
          a_local_csv:
            csv_local: ./example/testdata.csv
            columns:
              - value: amount
              - state
              - date
            dt_index: date
          same_but_different:
            csv_local: ./example/testdata.csv
            columns:
              - amount
              - location: state
              - date
            dt_index: date
        """
        store = Datastore(config)
        store.update()
        df1 = store.a_local_csv.get_df()
        df1 = df1.rename(columns={c: f'a_local_csv.{c}' for c in df1.columns})
        df2 = store.same_but_different.get_df()
        df2 = df2.rename(columns={c: f'same_but_different.{c}' for c in df2.columns})
        combined = store.combined
        self.assertEqual(len(df1), len(combined))
        self.assertTrue(combined.equals(pd.concat([df1, df2], axis=1)))

    def test_incremental(self):
        # FIXME TODO
        # create a proper incremental scenario,
        # this test breaks with the new handling of not storing identical files
        return

        config = """
        storage:
          filesystem:
            data_root: datastore-testdata/test_incremental
        datasets:
          my_dataset:
              csv_url: https://docs.google.com/spreadsheets/d/e/2PACX-1vRhzhiVJr0XPcMANnb9_F7bcE6h-C5826MGJs034AocLpyo4uy0y97LIG2ns8F1heCrSTsyEkL1XwDK/pub?output=csv  # noqa
              columns:
                - id: identifier
                - value
                - date
              incremental: true
              ops: false  # disable drop_duplicates to simulate updated data
        """
        store = Datastore(config)
        ds = store.my_dataset
        self.assertTrue(ds.config.incremental)
        items = len(ds.get_df())
        ds = ds.update()
        self.assertGreater(len(ds.get_df()), items)
        self.assertEqual(len(ds.get_df()), items*2)

        config = store.config.to_dict()
        del config['datasets']['my_dataset']['ops']  # enable default ops with drop_duplicates
        store = Datastore(config)
        ds = store.my_dataset
        self.assertTrue(ds.config.incremental)
        items = len(ds.get_df())
        ds = ds.update()
        self.assertEqual(len(ds.get_df()), items)

    def test_ops(self):
        config = """
        storage:
          filesystem:
            data_root: datastore-testdata/test_incremental
        datasets:
          my_dataset:
              csv_local: ./example/testdata.csv
              dt_index: date
        """
        store = Datastore(config)
        ds = store.datasets[0]
        self.assertIsInstance(ds.config.ops, list)  # base ops
        config = store.config.to_dict()
        config['datasets']['my_dataset']['ops'] = [
            {'sort_values': {'ascending': False, 'by': 'state'}},
            {'fillna': {'value': ''}},
            {'applymap': {'func': 'lambda x: x.lower() if isinstance(x, str) else x'}}
        ]
        store = Datastore(config)
        ds = store.datasets[0]
        df = ds.get_df()
        self.assertTrue(all(df['state'].map(lambda x: x.islower())))

        # unsafe eval raise
        config['datasets']['my_dataset']['ops'] = [
            {'applymap': {'func': "__import__('os').system('rm -rf /tmp/still-dont-be-too-risky-in-this-test')"}}
        ]
        store = Datastore(config)
        ds = store.datasets[0]
        self.assertRaises(NameError, ds.get_df)

    def test_json_dtype(self):
        store = self.store
        df = store.a_local_json.get_df()
        self.assertTrue(df['value'].dtype.name == 'object')

    def test_columns_map(self):
        ds = self.store.a_local_json
        df = ds.get_df()
        self.assertTrue(all(df['state'].map(lambda x: x.isupper())))
