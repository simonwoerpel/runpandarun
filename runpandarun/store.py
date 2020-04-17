import os

from . import combine
from .config import Config
from .exceptions import ConfigError
from .dataset import Dataset
from .storage import get_storage
from .util import cached_property


class Datastore:
    """
    Configure a set of datasets:

    yaml config. See `./examples/`

        storage:
          filesystem:
            data_root: /tmp/datastore/
        my_dataset:
          csv_url: https://docs.google.com/spreadsheets/d/e/2PACX-1vRhzhiVJr0XPcMANnb9_F7bcE6h-C5826MGJs034AocLpyo4uy0y97LIG2ns8F1heCrSTsyEkL1XwDK/pub?output=csv    # noqa
          columns:
            - id: identifier  # rename original column `identifier` to `id`
            - name
            - date
          index: id
    """
    def __init__(self, config=None):
        if config is None:
            config = os.getenv('CONFIG')
            if config is None:
                raise ConfigError('Please specify path to config yaml to `Datastore` or set via env var.')
        self.config = Config(config)
        self._storage = get_storage(config)
        self._datasets = self.config.get('datasets', {})
        self._combine = self.config.get('combine', [])
        self.validate()
        for dataset in self:
            setattr(self, dataset.name, dataset)

    def __iter__(self):
        for dataset in self.datasets:
            yield dataset

    def __contains__(self, item):
        return item in self._datasets

    def __repr__(self):
        return f'<Datastore: `{self._storage.backend.__class__.__name__}` {self._storage.backend}>'

    def __getitem__(self, attr):
        if attr in self:
            return getattr(self, attr)
        raise KeyError(f'Dataset `{attr}` not in `{self}`')

    @cached_property
    def datasets(self):
        return [Dataset(name, config, self) for name, config in self._datasets.items()]

    @cached_property
    def combined(self):
        """
        get a combined df from all datasets specified in combine config

        try:
          - concat long: if same index name and same column names
          - concat wide: if identical index
          - merge on index
        """
        datasets = [ds for ds in self if ds.name in self._combine]
        dfs = [ds._df for ds in datasets]
        # concat long
        if combine.test_index_name_equal(dfs) and combine.test_columns_equal(dfs):
            return combine.concat_long(dfs)
        # concat wide
        if combine.test_index_equal(dfs):
            return combine.concat_wide(datasets)

    @property  # not cached
    def last_update(self):
        return self._storage.last_update

    @property  # not cached
    def last_complete_update(self):
        return self._storage.last_complete_update

    def update(self):
        for dataset in self:
            dataset.update()
        self._storage.set_ts('last_complete_update')
        self._storage.set_ts('last_update')

    def validate(self):
        if not len(self._datasets):
            raise ConfigError(f'No datasets in config for `{self}`')
        if self._combine:
            for dataset in self._combine:
                if dataset not in self:
                    raise ConfigError(f'Cannot combine: dataset `{dataset}` not in `{self}`')
