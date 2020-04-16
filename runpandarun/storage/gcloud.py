import os
from google.cloud import storage, exceptions

from .backend import Backend
from ..exceptions import ConfigError
from ..util import cached_property


class GoogleCloudBackend(Backend):
    _is_cloud = True
    _default_location = 'europe-west3'

    def __str__(self):
        return self._bucket.name

    @cached_property
    def _client(self):
        if not os.getenv('GOOGLE_APPLICATION_CREDENTIALS'):
            raise ConfigError('Please specify env `GOOGLE_APPLICATION_CREDENTIALS` to correct json file')
        return storage.Client()

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
        # FIXME storage path handling
        # return f'gs://{self._bucket.name}/'
        return ''

    def exists(self, path):
        blob = self._get_blob(path)
        return blob.exists()

    def store(self, path, content, publish=False):
        ext = os.path.splitext(path)[1]
        if ext == '.csv':
            content_type = 'text/csv'
        elif ext == '.json':
            content_type = 'application/json'
        else:
            content_type = 'text/plain'
        blob = self._get_blob(path)
        blob.upload_from_string(content, content_type=content_type)
        if publish:
            if self.config.cache_control:
                blob.cache_control = self.config.cache_control
                blob.patch()
            blob.make_public()
            return blob.public_url
        return self.get_path(path)

    def _fetch(self, path):
        blob = self._get_blob(path)
        content = blob.download_as_string()
        return content.decode()

    def get_children(self, path, condition=lambda x: True):
        for blob in self._client.list_blobs(self._bucket, prefix=path):
            if condition(blob.name):
                yield os.path.split(blob.name)[-1], blob.name
