import os

from ..util import get_files
from .backend import Backend


def ensure_directory(fp):
    fp = os.path.abspath(fp)
    if not os.path.isdir(fp):
        os.makedirs(fp)
    return fp


class FilesystemBackend(Backend):
    def get_base_path(self):
        return ensure_directory(self.config.data_root)

    def exists(self, path):
        p = self.get_path(path)
        return any((os.path.isfile(p), os.path.isdir(p)))

    def store(self, path, content, publish=False):
        path = self.get_path(path)
        ensure_directory(os.path.split(path)[0])
        with open(path, 'w') as f:
            f.write(content)
        return path

    def _fetch(self, path):
        path = self.get_path(path)
        with open(path) as f:
            content = f.read().strip()
        return content

    def get_children(self, path, condition=lambda x: True):
        path = self.get_path(path)
        return get_files(path, condition)
