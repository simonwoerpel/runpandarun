import banal
import pandas as pd

from .exceptions import ConfigError


def apply_index(df, config):
    dt_index = config.dt_index
    if dt_index:
        if banal.is_mapping(dt_index):
            if 'column' not in dt_index:
                raise ConfigError('`dt_index` setting not valid. please specify at least `column`, either as mapping or as simple string value')  # noqa
            col = dt_index['column']
            index_conf = {k: v for k, v in dt_index.items() if k != 'column'}
            df.index = pd.DatetimeIndex(pd.to_datetime(df[col], **index_conf))
            del df[col]
            return df
        df.index = pd.DatetimeIndex(pd.to_datetime(df[dt_index]))
        del df[dt_index]
        return df
    df.index = df[config.index]
    del df[config.index]
    return df
