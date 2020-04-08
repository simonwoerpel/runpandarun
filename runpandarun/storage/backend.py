import os


class Backend:
    _is_cloud = False

    def __init__(self, config):
        self.config = config
        self.base_path = self.get_base_path()

    def __str__(self):
        return self.get_base_path()

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self}>'

    def get_base_path(self):
        # return a base path to a local file dir or a cloud bucket
        raise NotImplementedError

    def get_path(self, path):
        # return absolute filesystem path or cloud bucket for `path
        return os.path.join(self.base_path, path)

    def exists(self, path):
        # check if given path exists and return boolean
        raise NotImplementedError

    def store(self, path, content, publish=False):
        # store `content` in path and return absolute path to stored file or clouud blob location
        raise NotImplementedError

    def fetch(self, path):
        # return content as string for given path, use the same not found exception for all storages:
        if not self.exists(path):
            raise FileNotFoundError(f'Path `{path}` not found in storage `{self}`')
        return self._fetch(path)

    def _fetch(self, path):
        # actual implementation for specific storage
        raise NotImplementedError

    def set_value(self, path, value):
        # simply store values to a path location
        self.store(path, value)
        return value

    def get_value(self, path, transform=lambda x: x):
        # simply get values from a path location
        if not self.exists(path):
            return
        content = self.fetch(path)
        return transform(content)

    def get_children(self, path='/', condition=lambda x: True):
        # list all children under given path that match condition
        raise NotImplementedError
