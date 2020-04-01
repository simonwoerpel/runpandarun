import banal
import os
import google.cloud.storage as gcloud_storage
from urllib.parse import urljoin

from .exceptions import ConfigError
from .util import ensure_directory, cached_property


class BaseHandler:
    label = None

    def __init__(self, dataset, df, config):
        self.enabled = banal.as_bool(config.enabled)
        self.dataset = dataset
        self.df = df
        self.config = config
        self.name = config.get('name', dataset.name)
        self.format = config.get('format', dataset.format)
        self.overwrite = config.get('overwrite')
        self.dump = getattr(df, f'to_{self.format}')

    def publish(self):
        if self.enabled is False:
            return f'Publish handler `{self.label}` is not enabled in this environment`'
        exists = self.check_exists()
        if exists and self.overwrite not in (True, False):
            raise FileExistsError(f'File `{self.get_file_path()}` already exists in {self.label}')
        if not exists or (exists and self.overwrite):
            return self.store()

    def get_file_name(self):
        return f'{self.name}.{self.format}'

    def get_file_path(self):
        return f'{self.dataset.name}/{self.get_file_name()}'

    def check_exists(self):
        raise NotImplementedError

    def store(self):
        raise NotImplementedError


class FileSystemHandler(BaseHandler):
    label = 'filesystem'

    @cached_property
    def base_dir(self):
        return ensure_directory(os.path.abspath(os.path.join(self.config.public_root, self.dataset.name)))

    def get_abs_path(self):
        return os.path.join(self.base_dir, self.get_file_name())

    def check_exists(self):
        return os.path.isfile(self.get_abs_path())

    def store(self):
        self.dump(self.get_abs_path(), **(self.config.pd_args or {}))
        if self.config.include_source:
            source_fp = os.path.join(self.base_dir, f'source.{self.dataset.format}')
            with open(source_fp, 'w') as f:
                f.write(self.dataset._storage.get_source())
        if self.config.public_url:
            return urljoin(self.config.base_url, self.get_file_name())
        return self.get_abs_path()


class GoogleCloudHandler(BaseHandler):
    label = 'gcloud'

    @cached_property
    def blob(self):
        storage_client = gcloud_storage.Client()
        bucket = storage_client.bucket(self.config.bucket_name)
        return bucket.blob(self.get_file_path())

    def check_exists(self):
        return self.blob.exists()

    def store(self):
        content_type = 'text/csv'
        if self.format == 'json':
            content_type = 'application/json'
        content = self.dump(**self.config.pd_args)
        self.blob.upload_from_string(content, content_type, )
        self.blob.make_public()
        return self.blob.public_url


HANDLERS = {c.label: c for c in (FileSystemHandler, GoogleCloudHandler)}


def _publish(dataset, df, config, **kwargs):
    for handler_name, handler_config in config.publish.items():
        handler = HANDLERS.get(handler_name)
        if handler is None:
            raise ConfigError(f'Publish `{dataset}`: handler `{handler}` not valid')
        _config = config.update(handler_config).update(kwargs)
        handler = handler(dataset, df, _config)
        yield handler.publish()


def publish(dataset, df, config, **kwargs):
    return [res for res in _publish(dataset, df, config, **kwargs)]
