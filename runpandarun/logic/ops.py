from typing import Any, TypeAlias

from pandas import DataFrame

from ..exceptions import SpecError
from ..util import safe_eval

Operation: TypeAlias = dict[str, dict[str, Any] | str]


def apply(df: DataFrame, ops: list[Operation]):
    """
    apply any valid operation from `pd.DataFrame.<op>` with kwargs in given
    order
    """
    for op in ops:
        if len(op) > 1:
            raise SpecError("Operation not valid, can be only 1 key (name) per item.")
        name, kwargs = tuple(op.items())[0]
        kwargs = {k: safe_eval(v) if k == "func" else v for k, v in kwargs.items()}
        func = getattr(DataFrame, name, None)
        if func is None or not callable(func):
            raise SpecError(f"{op} is not a valid operation for `pd.DataFrame`")
        df = func(df, **kwargs)
    return df
