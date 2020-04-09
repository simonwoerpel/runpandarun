import banal
import os
from datetime import datetime
from urllib.parse import urljoin

from .exceptions import ConfigError
from .ops import apply_ops
from .storage import get_backend


class Handler:
    def __init__(self, dataset, df, config, backend):
        self.enabled = banal.as_bool(config.enabled)
        self.dataset = dataset
        self.config = config
        self.backend = backend
        self.name = config.get('name', dataset.name)
        self.format = config.get('format', dataset.format)
        self.overwrite = config.get('overwrite')
        self.with_timestamp = config.get('with_timestamp')
        self.base_path = self.get_base_path()
        df = apply_ops(df, config.get('clean', {}))
        self.dump = getattr(df, f'to_{self.format}')

    def get_base_path(self):
        return self.backend.get_path(self.dataset.name)

    def _fp(self, path=''):
        return os.path.join(self.base_path, path)

    def publish(self):
        if self.enabled is False:
            return f'Publish backend `{self.backend}` is not enabled in this environment`'
        fp = self._fp(self.get_file_name())
        exists = self.backend.exists(fp)
        if exists and self.overwrite not in (True, False):
            raise FileExistsError(f'File `{fp}` already exists in {self.backend}')
        if not exists or (exists and self.overwrite):
            if self.config.include_source:
                source_fp = self._fp(f'source.{self.dataset.format}')
                content = self.dataset._storage.get_source()
                self.backend.store(source_fp, content, publish=True)
            content = self.dump(**(self.config.pd_args or {}))
            res = self.backend.store(fp, content, publish=True)
            if self.config.public_url:
                return urljoin(self.config.base_url, fp)
            return res

    def get_file_name(self):
        if self.with_timestamp:
            ts = datetime.now().isoformat()
            return f'{self.name}.{ts}.{self.format}'
        return f'{self.name}.{self.format}'


def _publish(dataset, df, config, **kwargs):
    if config.publish is None:
        raise ConfigError('Add a publish handler config to be able to publish datasets.')
    for handler, handler_config in config.publish['handlers'].items():
        if banal.as_bool(handler_config.get('enabled', True)):
            backend = get_backend(handler, handler_config)
            _config = config.update(handler_config).update(kwargs)
            handler = Handler(dataset, df, _config, backend)
            yield handler.publish()


def publish(dataset, df, config, **kwargs):
    return [res for res in _publish(dataset, df, config, **kwargs)]
