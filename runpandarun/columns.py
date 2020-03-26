from .exceptions import ConfigError


def wrangle_columns(df, config):
    use_columns = []
    rename_columns = {}
    for column in config.columns:
        if isinstance(column, str):
            use_columns.append(column)
        elif isinstance(column, dict):
            if len(column) > 1:
                raise ConfigError(f'Column config `{column}` has errors.')
            target, source = list(column.items())[0]
            use_columns.append(source)
            rename_columns[source] = target
        else:
            raise ConfigError(f'Column config `{column}` has errors.')

    df = df[use_columns]
    if rename_columns:
        df = df.rename(columns=rename_columns)
    return df
