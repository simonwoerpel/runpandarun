import io
import json

import pandas as pd


def _load_csv(source):
    return pd.read_csv(io.StringIO(source))


def load_csv(source, config):
    if config.incremental:
        return pd.concat(_load_csv(s) for s in source)
    return _load_csv(source)


def _load_json(source, config):
    data = json.loads(source)
    normalize = config.json_normalize
    if normalize:
        return pd.json_normalize(data, **normalize)
    return pd.json_normalize(data)


def load_json(source, config):
    if config.incremental:
        return pd.concat(_load_json(s, config) for s in source)
    return _load_json(source, config)
