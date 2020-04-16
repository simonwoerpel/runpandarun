"""
test google cloud storage and publish
"""

import banal
import os
import requests
import unittest
import pandas as pd

from google.cloud.storage import Client
from google.cloud.exceptions import NotFound

from runpandarun import Datastore


class Test(unittest.TestCase):
    def setUp(self):
        os.environ['FILESYSTEM_PUBLISH_ENABLED'] = '0'
        os.environ['FILESYSTEM_ENABLED'] = '0'
        os.environ['GOOGLE_PUBLISH_ENABLED'] = '1'
        os.environ['GOOGLE_ENABLED'] = '1'
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'credentials.json'
        os.environ['CONFIG'] = './example/config.yml'
        self.store = Datastore()
        self.publish_config = self.store.config.publish['handlers']['gcloud']
        self.storage_config = self.store.config.storage['gcloud']
        self.dataset = self.store.datasets[0]
        self.client = Client()

    def tearDown(self):
        os.environ['FILESYSTEM_PUBLISH_ENABLED'] = '1'
        os.environ['FILESYSTEM_ENABLED'] = '1'
        os.environ['GOOGLE_PUBLISH_ENABLED'] = '0'
        os.environ['GOOGLE_ENABLED'] = '0'
        # delete created google buckets
        for bucket in (self.publish_config['bucket'], self.storage_config['bucket']):
            try:
                bucket = self.client.get_bucket(bucket)
                bucket.delete(force=True)
            except NotFound:
                pass

    def test_1config(self):
        self.assertTrue(banal.as_bool(self.publish_config['enabled']))
        self.assertTrue(banal.as_bool(self.storage_config['enabled']))
        self.assertEqual('runpandarun-testbucket-publish', self.publish_config['bucket'])
        self.assertEqual('runpandarun-testbucket-storage', self.storage_config['bucket'])

    def test_2implicit_bucket_creation(self):
        # storage bucket implicit created by storage init
        dataset = self.store.datasets[0]
        bucket = dataset._storage.backend._bucket
        _bucket = self.client.get_bucket(self.storage_config['bucket'])
        self.assertTrue(_bucket.exists())
        self.assertTrue(bucket.exists())
        self.assertEqual(bucket.id, _bucket.id)
        self.assertEqual(bucket.path, _bucket.path)
        self.assertEqual(bucket.time_created, _bucket.time_created)

        # publish bucket not created yet
        self.assertRaises(NotFound, self.client.get_bucket, self.publish_config['bucket'])

    def test_gcloud_storage(self):
        self.store.update()
        ds = self.store.datasets[0]
        df = ds.get_df()
        self.assertIsInstance(df, pd.DataFrame)

    def test_gcloud_publish(self):
        ds = self.store.datasets[0]
        res = ds.publish()
        url = 'https://storage.googleapis.com/runpandarun-testbucket-publish/my_dataset/my_dataset.csv'
        self.assertEqual(len(res), 1)
        self.assertEqual(url, res[0])

        # test overwrite
        self.assertRaises(FileExistsError, ds.publish)

        res = ds.publish(overwrite=True)
        self.assertEqual(url, res[0])

    def test_gcloud_cache(self):
        config = self.store.config.to_dict().copy()
        config['publish']['handlers']['gcloud']['cache_control'] = 'no-cache'
        store = Datastore(self.store.config.update(config))
        dataset = store.datasets[0]
        dataset.publish()
        url = 'https://runpandarun-testbucket-publish.storage.googleapis.com/my_dataset/my_dataset.csv'
        res = requests.get(url)
        self.assertEqual(res.headers['cache-control'], 'no-cache')
