import os
import pandas as pd

from .util import cached_property, ensure_directory, get_files


class DatasetRevisions:
    def __init__(self, dataset):
        self._dataset = dataset
        self._data_root = ensure_directory(os.path.join(dataset._storage.data_root, 'revisions'))

    def show(self):
        return self._names

    def __getitem__(self, item):
        if os.path.isfile(self._fp(item)):
            return pd.read_csv(self._fp(item), index_col=0)
        raise FileNotFoundError('Revision `{name}` not found for dataset `{self._dataset}`')

    def __setitem__(self, name, df):
        df.to_csv(self._fp(name))

    def __iter__(self):
        for _, fp in self._files:
            yield pd.read_csv(fp)

    def __contains__(self, item):
        return item in self._names

    def _fp(self, name):
        return os.path.join(self._data_root, f'{name}.csv')

    @cached_property
    def _files(self):
        return [f for f in get_files(self._data_root, lambda x: x.endswith('.csv'))]

    @cached_property
    def _names(self):
        return [n for n, _ in self._files]
