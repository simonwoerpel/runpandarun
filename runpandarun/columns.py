import banal

from .exceptions import ConfigError
from .util import safe_eval


def wrangle_columns(df, config):
    use_columns = []
    rename_columns = {}
    map_funcs = {}
    for column in config.columns:
        if isinstance(column, str):
            use_columns.append(column)
        elif banal.is_mapping(column):
            if len(column) > 1:
                raise ConfigError(f'Column config `{column}` has errors.')
            target, source = list(column.items())[0]
            if banal.is_mapping(source):
                source_column = source.get('column', target)
                map_func = source.get('map')
                if map_func:
                    map_funcs[target] = safe_eval(map_func)
            else:
                source_column = source
            use_columns.append(source_column)
            rename_columns[source_column] = target
        else:
            raise ConfigError(f'Column config `{column}` has errors.')

    df = df[use_columns]
    if rename_columns:
        df = df.rename(columns=rename_columns)
    if map_funcs:
        for col, func in map_funcs.items():
            df[col] = df[col].map(func)
    return df
