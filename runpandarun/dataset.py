import numpy as np
import pandas as pd

from . import publish, load, index, columns, ops
from .config import Config
from .exceptions import ConfigError
from .revisions import DatasetRevisions
from .storage import DatasetStorage
from .util import cached_property


RESAMPLE_METHODS = {
    'sum': np.sum,
    'mean': np.mean,
    'max': np.max,
    'min': np.min,
    'count': np.count_nonzero
}

RESAMPLE_INTERVALS = {
    'minutely': '1T',
    'hourly': '1H',
    'daily': '1D',
    'weekly': '1W',
    'monthly': '1M',
    'yearly': '1A'
}

DEFAULT_CONFIG = {
    'index': 'id',
    'ops': [
        'drop_duplicates',
        'sort_index'
    ]
}


# aggregation shortcuts
class Resample:
    def __init__(self, interval, resample):
        for method in RESAMPLE_METHODS.keys():
            def resample_func():
                return resample(interval, method)
            setattr(self, method, resample_func)


class Dataset:
    def __init__(self, name, config, store):
        self.name = name
        self.config = Config({**DEFAULT_CONFIG, **config})
        self.store = store
        self._storage = DatasetStorage(name, config, store._storage)
        self.revisions = DatasetRevisions(self)

        # provide handy pd shortcuts
        for interval_name, interval in RESAMPLE_INTERVALS.items():
            setattr(self, interval_name, Resample(interval, self.resample))

    def __str__(self):
        return self.name

    def __repr__(self):
        return f'<Dataset: {self.name}>'

    def __getitem__(self, item):
        return self.revisions[item]

    def __setitem__(self, name, item):
        # secret hack, should not be used by real people
        self.revisions.save(name, item)

    @cached_property
    def _df(self):
        return self.get_df()

    @cached_property
    def format(self):
        return self._storage.format

    @cached_property
    def is_json(self):
        return self._storage.is_json

    @cached_property
    def is_csv(self):
        return self._storage.is_csv

    @cached_property
    def url(self):
        return self._storage.remote_url

    def update(self):
        """refresh from remote source and return new instance"""
        self._storage.get_source(update=True)
        return Dataset(self.name, self.config.to_dict(), self.store)

    def load(self):
        source = self._storage.get_source()

        if self.is_csv:
            return load.load_csv(source, self.config)

        if self.is_json:
            return load.load_json(source, self.config)

    def publish(self, df=None, **kwargs):
        if df is None:
            df = self._df
        config = self.config.update({'publish': self.store.config.publish or {}})  # FIXME hrmpf
        return publish.publish(self, df, config, **kwargs)

    def get_df(self):
        df = self.load()

        if self.config.columns:
            df = columns.wrangle_columns(df, self.config)

        if self.config.ops:
            df = ops.apply_ops(df, self.config.ops)

        df = index.apply_index(df, self.config)

        return df

    def resample(self, interval, method):
        if not self.config.dt_index:
            raise ConfigError(f'Dataset `{self.name}` has no `DatetimeIndex` configured.')
        if method not in RESAMPLE_METHODS.keys():
            raise ConfigError(f'Resampling method `{method}` not valid.')  # noqa
        if method == 'count':  # FIXME implementation?
            df = self._df.copy()
            df['count'] = 1
            return df.resample(interval)[['count']].count()
        return self._df[self.numeric_cols()].resample(interval).apply(RESAMPLE_METHODS[method])

    def numeric_cols(self):
        for col in self._df.columns:
            if pd.api.types.is_numeric_dtype(self._df[col]):
                yield col
