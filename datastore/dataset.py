import io
import numpy as np
import pandas as pd

from .exceptions import ConfigError
from .storage import DatasetStorage
from .util import cached_property


RESAMPLE_METHODS = {
    'sum': np.sum,
    'mean': np.mean,
    'max': np.max,
    'min': np.min
}

RESAMPLE_INTERVALS = {
    'minutely': '1T',
    'hourly': '1H',
    'daily': '1D',
    'weekly': '1W',
    'monthly': '1M',
    'yearly': '1A'
}


# aggregation shortcuts
class Resample:
    def __init__(self, interval, resample):
        for method in RESAMPLE_METHODS.keys():
            def resample_func():
                return resample(interval, method)
            setattr(self, method, resample_func)


class Dataset:
    def __init__(self, name, config, storage):
        self.name = name
        self._config = config
        self._storage = DatasetStorage(name, config, storage)
        self._base_df = None
        self._df = None
        self._has_dt_index = config.get('dt_index', False) is True
        self._incremental = config.get('incremental', False) is True
        self._drop_duplicates = config.get('drop_duplicates', False) is True

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
        return Dataset(self.name, self._config, self._storage.storage)

    def load(self):
        source = self._storage.get_source()
        if self._storage.is_csv:
            if self._incremental:
                return pd.concat(pd.read_csv(io.StringIO(d)) for d in source)
            data = io.StringIO(self._storage.get_source())
            return pd.read_csv(data)
        if self._storage.is_json:
            if self._incremental:
                return pd.concat(pd.read_json(io.StringIO(d)) for d in source)
            data = io.StringIO(self._storage.get_source())
            return pd.read_json(data)

    def get_df(self):
        index = self._config['index']
        columns = self._config['columns']
        use_columns = []
        rename_columns = {}
        for column in columns:
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

        df = self.load()
        df = df[use_columns]

        if rename_columns:
            df = df.rename(columns=rename_columns)

        if self._has_dt_index:
            df.index = pd.DatetimeIndex(pd.to_datetime(df[index]))
        else:
            df.index = df[index]
        del df[index]
        df = df.sort_index()

        if self._drop_duplicates:
            df = df.drop_duplicates()

        return df

    def resample(self, interval, method):
        if not self._has_dt_index:
            raise ConfigError(f'Dataset `{self.name}` has no `DatetimeIndex` configured.')
        if method not in RESAMPLE_METHODS.keys():
            raise ConfigError(f'Resampling method `{method}` not valid.')  # noqa
        return self.df[self.numeric_cols()].resample(interval).apply(RESAMPLE_METHODS[method])

    def numeric_cols(self):
        for col in self.df.columns:
            if pd.api.types.is_numeric_dtype(self.df[col]):
                yield col
