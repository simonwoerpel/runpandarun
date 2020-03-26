import io
import numpy as np
import pandas as pd

from . import publish as publish_ops
from .config import Config
from .exceptions import ConfigError
from .ops import apply_ops
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
        self._base_df = None
        self._df = None

        for interval_name, interval in RESAMPLE_INTERVALS.items():
            setattr(self, interval_name, Resample(interval, self.resample))

    def __str__(self):
        return self.name

    def __repr__(self):
        return f'<Dataset: {self.name}>'

    @cached_property
    def df(self):
        return self.get_df()

    def update(self):
        """refresh from remote source and return new instance"""
        self._storage.get_source(update=True)
        return Dataset(self.name, self.config.to_dict(), self.store)

    def load(self):
        source = self._storage.get_source()
        read = getattr(pd, f'read_{self._storage.format}')
        if self.config.incremental:
            return pd.concat(read(io.StringIO(d)) for d in source)
        return read(io.StringIO(source))

    def publish(self, df=None, **kwargs):
        if df is None:
            df = self.df
        config = self.config.update(self.store.config.publish or {})  # FIXME hrmpf
        return publish_ops.filesystem_publish(self, df, config, **kwargs)

    def get_df(self):
        df = self.load()

        if self.config.columns:
            use_columns = []
            rename_columns = {}
            for column in self.config.columns:
                if isinstance(column, str):
                    use_columns.append(column)
                elif isinstance(column, dict):
                    if len(column) > 1:
                        raise ConfigError(f'Column mapping for dataset `{self.name}` has errors.')
                    target, source = list(column.items())[0]
                    use_columns.append(source)
                    rename_columns[source] = target
                else:
                    raise ConfigError(f'Column mapping for dataset `{self.name}` has errors.')

            df = df[use_columns]
            if rename_columns:
                df = df.rename(columns=rename_columns)

        if self.config.ops:
            df = apply_ops(df, self.config.ops)

        index = self.config.dt_index or self.config.index
        if index not in df.columns:
            raise ConfigError(f'Please specify a valid index column for `{self.name}`. `{index}` is not valid')
        if self.config.dt_index:
            df.index = pd.DatetimeIndex(pd.to_datetime(df[index]))
        else:
            df.index = df[index]
        del df[index]

        return df

    def resample(self, interval, method):
        if not self.config.dt_index:
            raise ConfigError(f'Dataset `{self.name}` has no `DatetimeIndex` configured.')
        if method not in RESAMPLE_METHODS.keys():
            raise ConfigError(f'Resampling method `{method}` not valid.')  # noqa
        if method == 'count':  # FIXME implementation?
            df = self.df.copy()
            df['count'] = 1
            return df.resample(interval)[['count']].count()
        return self.df[self.numeric_cols()].resample(interval).apply(RESAMPLE_METHODS[method])

    def numeric_cols(self):
        for col in self.df.columns:
            if pd.api.types.is_numeric_dtype(self.df[col]):
                yield col
