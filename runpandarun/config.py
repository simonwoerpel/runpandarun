class Config:
    """simple wrapper to handle config dicts conveniently"""
    def __init__(self, config):
        self._config = config

    def __getattr__(self, attr):
        return self._config.get(attr)

    def __get__(self, attr, default=None):
        return self._config.get(attr, default or {})

    def get(self, attr, default=None):
        return self.__get__(attr, default)

    def to_dict(self):
        return self._config

    def update(self, data):
        return self.__class__({**self._config, **data})
