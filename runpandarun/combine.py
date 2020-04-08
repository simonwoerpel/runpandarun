import pandas as pd
import numpy as np


def test_index_equal(dfs):
    df = dfs[0]
    return all(np.array_equal(df.index, _df.index) for _df in dfs[1:])


def test_index_name_equal(dfs):
    return len(set(df.index.name for df in dfs)) == 1


def test_columns_equal(dfs):
    df = dfs[0]
    return all(np.array_equal(df.columns, _df.columns) for _df in dfs[1:])


def concat_long(dfs):
    df = pd.concat(dfs)
    df = df.sort_index()
    return df


def concat_wide(datasets):
    dfs = (ds._df.rename(columns={c: f'{ds.name}.{c}' for c in ds._df.columns}) for ds in datasets)
    df = pd.concat(dfs, axis=1)
    return df
