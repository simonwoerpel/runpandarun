import os
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import storage, exceptions

from .backend import Backend
from ..util import cached_property


class GoogleCloudBackend(Backend):
    _default_location = 'europe-west3'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            self._client = storage.Client()
        except DefaultCredentialsError:
            self._client = None

    def __str__(self):
        return self._bucket.name

    @cached_property
    def _bucket(self):
        try:
            return self._client.get_bucket(self.config.bucket)
        except exceptions.NotFound:
            return self._client.create_bucket(
                self.config.bucket,
                location=self.config.location or self._default_location
            )

    def _get_blob(self, path):
        return self._bucket.blob(path)

    def get_base_path(self):
        return f'gs://{self._bucket.name}/'

    def exists(self, path):
        blob = self._get_blob(path)
        return blob.exists()

    def store(self, path, content):
        ext = os.path.splitext(path)[1]
        if ext == 'csv':
            content_type = 'text/csv'
        elif ext == 'json':
            content_type = 'application/json'
        else:
            content_type = 'text/plain'
        blob = self._get_blob(path)
        blob.upload_from_string(content, content_type)
        return self.get_path(path)

    def _fetch(self, path):
        blob = self._get_blob(path)
        return blob.download_as_string()

    def get_children(self, path, condition=lambda x: True):
        for blob in self._client.list_blobs(self._bucket, prefix=path):
            if condition(blob.name):
                yield os.path.split(blob.name)[-1], blob.path
