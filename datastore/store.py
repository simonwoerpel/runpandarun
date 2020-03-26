import yaml

from . import combine
from .exceptions import ConfigError
from .dataset import Dataset
from .storage import Storage
from .util import cached_property


class Datastore:
    """
    Configure a set of datasets:

    yaml config. See `./examples/`

        storage:
          data_root: /tmp/datastore/
        my_dataset:
          csv_url: https://docs.google.com/spreadsheets/d/e/2PACX-1vRhzhiVJr0XPcMANnb9_F7bcE6h-C5826MGJs034AocLpyo4uy0y97LIG2ns8F1heCrSTsyEkL1XwDK/pub?output=csv    # noqa
          columns:
            - id: identifier  # rename original column `identifier` to `id`
            - name
            - date
          index: id
    """
    def __init__(self, config):
        storage_config = config.get('storage', {})
        data_root = storage_config.get('data_root', './data/')
        self._config = config
        self._storage = Storage(data_root)
        self._datasets = config.get('datasets', {})
        self._combine = config.get('combine', [])
        self.validate()
        for dataset in self:
            setattr(self, dataset.name, dataset)

    def __iter__(self):
        for dataset in self.datasets:
            yield dataset

    def __contains__(self, item):
        return item in self._datasets

    def __repr__(self):
        return f'<Datastore: {self._storage.data_root}>'

    @cached_property
    def datasets(self):
        return [Dataset(name, config, self._storage) for name, config in self._datasets.items()]

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
        dfs = [ds.df for ds in datasets]
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

    @classmethod
    def from_yaml(cls, yaml_path):
        with open(yaml_path) as f:
            config = yaml.safe_load(f.read())
        return cls(config)

    @classmethod
    def from_yaml_string(cls, yaml_str):
        config = yaml.safe_load(yaml_str)
        return cls(config)

    @classmethod
    def from_dict(cls, config):
        return cls(config)

    def validate(self):
        if not len(self._datasets):
            raise ConfigError(f'No datasets in config for `{self}`')
        if self._combine:
            for dataset in self._combine:
                if dataset not in self:
                    raise ConfigError(f'Cannot combine: dataset `{dataset}` not in `{self}`')
