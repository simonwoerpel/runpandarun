import pandas as pd

from .exceptions import ConfigError
from .util import ensure_singlekey_dict, safe_eval


def _paginate_offset(get_request, offset_param, get_offset_value, offset=0):
    res = get_request(**{offset_param: offset})
    new_offset = offset + get_offset_value(res)
    if new_offset > offset:
        yield res
        yield from _paginate_offset(get_request, offset_param, get_offset_value, new_offset)


def fetch_paginated(get_request, config):
    config = config.get('method')
    method, config = ensure_singlekey_dict(config)
    if method is None:
        raise ConfigError(f'Please make sure {config} is properly configured as single-key dict!')
    if method != 'offset':
        raise ConfigError(f'Other pagination method than `{method}` currently not registered')
    yield from _paginate_offset(get_request, config['param'], safe_eval(config['get_offset_value']))


def get_resampled_versions(versions, config, incremental=False):
    resample_by = config.get('resample', '%Y-%m-%d')
    df = pd.DataFrame([(v.split('.')[1:2][0], v.split('.')[0].split('--')[1], v) for v in versions],
                      columns=('ts', 'page', 'version'))
    df['date'] = pd.to_datetime(df['ts']).dt.strftime(resample_by)
    df['page'] = df['page'].map(int)
    df = df.sort_values(['date', 'page']).drop_duplicates(subset=('date', 'page'), keep='last')
    if not incremental:
        # return only latest timestamp:
        latest = df['date'].max()
        df = df[df['date'] == latest]
    return df['version'].tolist()
