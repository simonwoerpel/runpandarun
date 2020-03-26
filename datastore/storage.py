import requests
import os

from datetime import datetime
from dateutil import parser

from .exceptions import FetchError
from .util import get_value_from_file, cached_property, ensure_directory


class Storage:
    """
    store & access a `dataset.Dataset`'s source data files,
    optionally with archive for historical data if it's enabled for
    a dataset (default: yes)

    it's an underlying helper class to handle the filesystem stuff.
    It will not be used by the user directly, they will use `datasets.Datastore`
    """
    def __init__(self, data_root):
        self.data_root = ensure_directory(os.path.abspath(data_root))

    def __repr__(self):
        return f'<Storage: {self.data_root}>'

    @property  # not cached
    def last_update(self):
        return self.get_ts('last_update')

    @property  # not cached
    def last_complete_update(self):
        return self.get_ts('last_complete_update')

    def set_ts(self, key, ts=None):
        ts = ts or datetime.utcnow()
        if not isinstance(ts, str):
            ts = ts.isoformat()
        fp = os.path.join(self.data_root, key)
        with open(fp, 'w') as f:
            f.write(ts)

    def get_ts(self, key):
        fp = os.path.join(self.data_root, key)
        return get_value_from_file(fp, transform=parser.parse)


class DatasetStorage(Storage):
    """Storage for a specific dataset"""
    def __init__(self, name, config, storage):
        self.name = name
        self.config = config
        self.storage = storage
        self.data_root = ensure_directory(os.path.join(storage.data_root, name))
        self.validate()

    def get_source(self, update=False, version='newest'):
        """
        return the source file content of the dataset
        """
        if update or self.should_update():
            self.fetch()
        versions = [v for v in sorted(os.listdir(self.data_root)) if 'last_update' not in v]

        if self.config.get('incremental', False) is True:
            # concat all the versions
            return self.get_incremental_sources(versions)

        if version == 'newest':
            fp = versions[-1]
        elif version == 'oldest':
            fp = versions[0]
        else:
            raise NotImplementedError(
                f'Currently only `newest` or `oldest` version is possible for dataset {self.name}'
            )
        with open(os.path.join(self.data_root, fp)) as f:
            content = f.read()
        return content

    def get_incremental_sources(self, versions):
        for fp in versions:
            with open(os.path.join(self.data_root, fp)) as f:
                content = f.read()
            yield content

    def fetch(self, store=True):
        """fetch a dataset source and store it on disk"""
        content = self.get_remote_content()
        if content:
            if store:
                ts = datetime.utcnow().isoformat()
                fp = os.path.join(self.data_root, 'data.%s.%s' % (ts, self.format))
                with open(fp, 'w') as f:
                    f.write(content)
                self.set_ts('last_update')
                self.storage.set_ts('last_update')
            return
        raise FetchError(f'Could not fetch source data for dataset `{self.name}`.')

    def get_remote_content(self):
        if self.is_remote:
            res = self.get_request()
            if res.ok:
                return res.text
            raise FetchError(
                f'Could not fetch source data from `{self.url}` for dataset `{self.name}`. {res.status_code}'
            )
        if self.is_local:
            with open(self.path) as f:
                content = f.read()
            return content

    def should_update(self):
        """determine if remote content should be fetched / updated"""
        contents = os.listdir(self.data_root)
        if not set(contents) - set(['last_update']):
            return True
        return self.last_update is None

    def get_request(self):
        url = self.url
        params = self.config.get('request', {}).get('params')
        headers = self.config.get('request', {}).get('headers')
        return requests.get(url, params=params, headers=headers)

    @cached_property
    def is_csv(self):
        return any((self.config.get('csv_url'), self.config.get('csv_local')))

    @cached_property
    def is_json(self):
        return any((self.config.get('json_url'), self.config.get('json_local')))

    @cached_property
    def is_remote(self):
        return any((self.config.get('json_url'), self.config.get('csv_url')))

    @cached_property
    def is_local(self):
        return any((self.config.get('json_local'), self.config.get('csv_local')))

    @cached_property
    def format(self):
        if self.is_csv:
            return 'csv'
        if self.is_json:
            return 'json'

    @cached_property
    def url(self):
        if self.is_remote:
            return self.config.get(f'{self.format}_url')

    @cached_property
    def path(self):
        if self.is_local:
            return self.config.get(f'{self.format}_local')

    def validate(self):
        """validate the config"""
        assert not all((self.is_remote, self.is_local))
        assert any((self.is_remote, self.is_local))
        assert not all((self.is_csv, self.is_json))
        assert any((self.is_csv, self.is_json))
        assert not all((self.url, self.path))
        assert self.format
        if self.is_remote:
            assert self.url, self.url
        if self.is_local:
            assert self.path, self.path
