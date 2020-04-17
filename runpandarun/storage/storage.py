import banal
import requests
import os

from datetime import datetime
from dateutil import parser

from ..config import Config
from ..exceptions import FetchError
from ..fetch import paginate
from ..util import cached_property, make_key


class Storage:
    """
    store & access a `dataset.Dataset`'s source data files,
    optionally with archive for historical data if it's enabled for
    a dataset (default: yes)

    it's an underlying helper class to handle the filesystem or cloud stuff.
    It will not be used by the user directly, they will use `datasets.Datastore`
    """
    def __init__(self, config, backend_class):
        self.config = Config(config)
        self.backend = backend_class(self.config)
        self.base_path = ''

    def __repr__(self):
        return f'<Storage: `{self.backend.__class__.__name__}` {self.backend}>'

    def _fp(self, path=''):
        return os.path.join(self.base_path, path)

    @property  # not cached
    def last_update(self):
        return self.get_ts('last_update')

    @property  # not cached
    def last_complete_update(self):
        return self.get_ts('last_complete_update')

    def set_ts(self, path, ts=None):
        path = self._fp(path)
        ts = ts or datetime.utcnow()
        if not isinstance(ts, str):
            ts = ts.isoformat()
        return self.backend.set_value(path, ts)

    def get_ts(self, path):
        path = self._fp(path)
        return self.backend.get_value(path, parser.parse)


class DatasetStorage(Storage):
    """Storage for a specific dataset"""
    def __init__(self, name, config, storage):
        self.name = name
        self.config = Config(config)
        self.storage = storage
        self.backend = storage.backend
        self.validate()
        self.base_path = self.name

    def get_source(self, update=False, version='newest'):
        """
        return the source file content of the dataset
        """
        if not self.should_store():
            return self.get_remote_content()

        if update or self.should_update():
            self.fetch(store=self.should_store())

        versions = self.backend.get_children(self._fp('data'))
        versions = sorted([v for _, v in versions])

        if self.config.incremental or self.config.paginate:
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

        # FIXME cloud storage path handling
        if self.backend._is_cloud:
            return self.backend.fetch(fp)
        return self.backend.fetch(self._fp(fp))

    def get_incremental_sources(self, versions):
        for fp in versions:
            # FIXME cloud storage path handling
            if self.backend._is_cloud:
                yield self.backend.fetch(fp)
            else:
                yield self.backend.fetch(self._fp(fp))

    def fetch(self, store=True):
        """fetch a dataset source and store it on disk"""
        content = self.get_remote_content()
        if content:
            if store:
                if self.config.paginate:
                    return self.store_paginated(content)
                return self.store(content)
        raise FetchError(f'Could not fetch source data for dataset `{self.name}`.')

    def get_remote_content(self):
        if self.is_remote:
            if self.config.paginate:
                return paginate(self.get_request, self.config.paginate)
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
        contents = len(list(self.backend.get_children(self._fp('data'))))
        if contents == 0:
            return True
        return self.last_update is None

    def should_store(self):
        """determine if remote content should be stored in `self.storage`"""
        if self.is_remote:
            return True
        if self.is_local:
            return banal.as_bool(self.config.copy)

    def get_request(self, **params):
        url = self.url
        params = {**self.config.get('request').get('params', {}), **params}
        headers = self.config.get('request').get('headers')
        return requests.get(url, params=params, headers=headers)

    def store(self, content, page=None):
        # still only store if newer file is different
        key_name = 'last_update_key'
        fp = 'data/data.%s.%s'
        if page is not None:
            key_name += f'--{page}'
            fp = f'data/data--{page}.%s.%s'
        last_key = self.backend.get_value(self._fp(key_name))

        key = make_key(content, hash=True)
        if last_key != key:
            ts = datetime.utcnow().isoformat()
            fp = fp % (ts, self.format)
            self.backend.store(self._fp(fp), content)
            self.backend.set_value(self._fp(key_name), key)
            self.set_ts('last_update')
            self.storage.set_ts('last_update')

    def store_paginated(self, results):
        for page, res in enumerate(results):
            self.store(res.text, page)

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

    @cached_property
    def remote_url(self):
        if self.is_remote:
            return self.url
        path = os.path.abspath(self.path)
        return f'file://{path}'

    def validate(self):
        """validate the config"""
        assert not all((self.is_remote, self.is_local))
        assert any((self.is_remote, self.is_local))
        assert not all((self.is_csv, self.is_json))
        assert any((self.is_csv, self.is_json))
        assert not all((self.url, self.path))
        assert self.format in ('csv', 'json')
        if self.is_remote:
            assert self.url, self.url
        if self.is_local:
            assert self.path, self.path
