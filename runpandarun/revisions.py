import base64
import os
import pickle


class DatasetRevisions:
    def __init__(self, dataset):
        self._dataset = dataset
        self._backend = dataset._storage.storage.backend
        self._base_path = 'revisions'

    def list(self):
        return [n for n, _ in self._get_files()]

    def __getitem__(self, item):
        fp = self._fp(item)
        if self._backend.exists(fp):
            content = self._backend.fetch(fp)
            return pickle.loads(base64.b64decode(content))
        raise FileNotFoundError(f'Revision `{item}` not found for dataset `{self._dataset}`')

    def __iter__(self):
        for item, _ in self._get_files():
            yield self[item]

    def __contains__(self, item):
        return item in self.list()

    def save(self, name, item):
        content = base64.b64encode(pickle.dumps(item)).decode()
        fp = self._fp(name)
        self._backend.store(fp, content)

    def _fp(self, name):
        return os.path.join(self._base_path, f'{name}.pkl')

    def _get_files(self):
        return [f for f in self._backend.get_children(self._base_path, lambda x: x.endswith('.pkl'))]
