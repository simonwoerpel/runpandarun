import banal
import io
import json

import pandas as pd


def _load_csv(source, config):
    return pd.read_csv(io.StringIO(source), **config)


def load_csv(source, config):
    if config.incremental or config.paginate:
        return pd.concat(_load_csv(s, config.read or {}) for s in source)
    return _load_csv(source, config.read or {})


def _load_json(source, config):
    data = json.loads(source)
    normalize = config.json_normalize or {}
    df = pd.json_normalize(data, **normalize)
    for col, dtype in banal.ensure_dict(config.get('read').get('dtype')).items():
        df[col] = df[col].astype(dtype)
    return df


def load_json(source, config):
    if config.incremental or config.paginate:
        return pd.concat(_load_json(s, config) for s in source)
    return _load_json(source, config)
