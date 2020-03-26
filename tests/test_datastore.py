import unittest

import pandas as pd

from datastore.dataset import Dataset, RESAMPLE_METHODS, RESAMPLE_INTERVALS
from datastore.store import Datastore
from datastore.storage import Storage


class Test(unittest.TestCase):
    def setUp(self):
        self.store = Datastore.from_yaml('./example/config.yml')

    def test_init(self):
        store = self.store
        self.assertIn('datastore-testdata', repr(store))
        self.assertIn('datastore-testdata', repr(store._storage))
        self.assertIn('datastore-testdata', store._storage.data_root)

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
        df = self.store.datasets[0].df
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual('id', df.index.name)

    def test_json(self):
        ds = self.store.a_local_json
        self.assertTrue(ds._has_dt_index)
        df = ds.df
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual('date', df.index.name)

    def test_dtindex(self):
        df = self.store.a_local_csv.df
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
        df1 = self.store.a_local_csv.df
        df2 = self.store.a_local_json.df
        combined = self.store.combined
        self.assertSetEqual(set(combined.columns), set(df1.columns))
        self.assertEqual(len(df1) + len(df2), len(combined))
        self.assertTrue(combined.equals(pd.concat([df1, df2]).sort_index()))

    def test_combine_wide(self):
        # add a dummy (copied) dataset
        config = """
        storage:
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
            index: date
            dt_index: true
          same_but_different:
            csv_local: ./example/testdata.csv
            columns:
              - amount
              - location: state
              - date
            index: date
            dt_index: true
        """
        store = Datastore.from_yaml_string(config)
        store.update()
        df1 = store.a_local_csv.df
        df1 = df1.rename(columns={c: f'a_local_csv.{c}' for c in df1.columns})
        df2 = store.same_but_different.df
        df2 = df2.rename(columns={c: f'same_but_different.{c}' for c in df2.columns})
        combined = store.combined
        self.assertEqual(len(df1), len(combined))
        self.assertTrue(combined.equals(pd.concat([df1, df2], axis=1)))

    def test_incremental(self):
        config = """
        storage:
          data_root: datastore-testdata/test_incremental
        datasets:
          my_dataset:
              csv_url: https://docs.google.com/spreadsheets/d/e/2PACX-1vRhzhiVJr0XPcMANnb9_F7bcE6h-C5826MGJs034AocLpyo4uy0y97LIG2ns8F1heCrSTsyEkL1XwDK/pub?output=csv  # noqa
              columns:
                - id: identifier
                - value
                - date
              index: id
              incremental: true
        """
        store = Datastore.from_yaml_string(config)
        ds = store.my_dataset
        self.assertTrue(ds._incremental)
        items = len(ds.df)
        ds = ds.update()
        self.assertGreater(len(ds.df), items)
        self.assertEqual(len(ds.df), items*2)

        config = store._config
        config['datasets']['my_dataset']['drop_duplicates'] = True
        store = Datastore.from_dict(config)
        ds = store.my_dataset
        self.assertTrue(ds._incremental)
        items = len(ds.df)
        ds = ds.update()
        self.assertEqual(len(ds.df), items)
