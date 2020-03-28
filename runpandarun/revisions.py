import os
import pickle

from .util import ensure_directory, get_files


class DatasetRevisions:
    def __init__(self, dataset):
        self._dataset = dataset
        self._data_root = ensure_directory(os.path.join(dataset._storage.data_root, 'revisions'))

    def list(self):
        return [n for n, _ in self._get_files()]

    def __getitem__(self, item):
        if os.path.isfile(self._fp(item)):
            with open(self._fp(item), 'rb') as f:
                return pickle.load(f)
        raise FileNotFoundError('Revision `{name}` not found for dataset `{self._dataset}`')

    def __setitem__(self, name, item):
        with open(self._fp(name), 'wb') as f:
            pickle.dump(item, f)

    def __iter__(self):
        for item, _ in self._get_files():
            yield self[item]

    def __contains__(self, item):
        return item in self.list()

    def _fp(self, name):
        return os.path.join(self._data_root, f'{name}.pkl')

    def _get_files(self):
        return [f for f in get_files(self._data_root, lambda x: x.endswith('.pkl'))]
