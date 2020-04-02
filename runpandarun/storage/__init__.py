import banal

from ..exceptions import ConfigError
from ..config import Config
from .storage import Storage, DatasetStorage  # noqa
from .filesystem import FilesystemBackend
from .gcloud import GoogleCloudBackend


BACKENDS = {
    'filesystem': FilesystemBackend,
    'gcloud': GoogleCloudBackend
}


def get_backend(backend_name, backend_config=None):
    if backend_name not in BACKENDS.keys():
        raise ConfigError(f'Backend `{backend_name}` not supported.')
    backend_class = BACKENDS[backend_name]
    if backend_config:
        config = Config(backend_config)
        return backend_class(config)
    return backend_class


def get_storage(config):
    # return storage based on config
    config = Config(config)
    backends = {k: v for k, v in config.storage.items() if banal.as_bool(v.get('enabled', True))}
    if len(backends) == 0:
        raise ConfigError('No storage backend active. Please enable one.')
    if len(backends) > 1:
        raise ConfigError('Currently only 1 active storage backend supported. Please disable others with `enabled: false` in your config')  # noqa
    backend_name, config = list(backends.items())[0]
    backend_class = get_backend(backend_name)
    return Storage(config, backend_class)
