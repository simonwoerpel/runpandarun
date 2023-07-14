from typing import Any, TypeAlias

import banal
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
        op_name = op
        op_args = None
        if banal.is_mapping(op):
            name = list(op.keys())
            if len(name) > 1:
                raise SpecError(f"Operation not valid: {name} - should be only 1 item.")
            op_name = name[0]
            op_args = list(op.values())
            if len(op_args) > 1:
                raise SpecError(
                    f"Operation arguments not valid: {op_args} - should be only 1 mapping item."
                )
            op_args = {
                k: safe_eval(v) if k == "func" else v for k, v in op_args[0].items()
            }
        func = getattr(DataFrame, op_name, None)
        if func is None or not callable(func):
            raise SpecError(f"{op} is not a valid operation for `pd.DataFrame`")
        if op_args:
            df = func(df, **op_args)
        else:
            df = func(df)
    return df
