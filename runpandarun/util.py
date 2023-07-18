import os
from datetime import datetime, timedelta
from functools import cache
from pathlib import Path
from typing import Any, TypeAlias
from urllib.parse import urlparse

import banal
import fingerprints
import normality
import numpy as np
import pandas as pd

PathLike: TypeAlias = str | os.PathLike[str] | Path


def safe_eval(value):
    return eval(
        str(value),
        {
            "__builtins__": {
                "pd": pd,
                "np": np,
                "str": str,
                "int": int,
                "float": float,
                "dict": dict,
                "list": list,
                "tuple": tuple,
                "None": None,
                "True": True,
                "False": False,
                "len": len,
                "hasattr": hasattr,
                "getattr": getattr,
                "isinstance": isinstance,
                "datetime": datetime,
                "timedelta": timedelta,
                "banal": banal,
                "normality": normality,
                "fingerprints": fingerprints,
            }
        },
    )


def expandvars(data: Any) -> dict[str, Any]:
    if isinstance(data, str):
        return os.path.expandvars(data)
    elif banal.is_listish(data):
        return [expandvars(v) for v in data]
    elif banal.is_mapping(data):
        return {k: expandvars(v) for k, v in dict(data).items()}
    return data


@cache
def absolute_path(
    path: PathLike, base: PathLike, py_module: bool | None = False
) -> PathLike | str:
    if path == "-" or urlparse(str(path)).scheme:
        return path
    path = (Path(base) / Path(path)).absolute()
    if py_module:
        path, rest = str(path).rsplit(":", 1)
        return Path(path).as_uri() + f":{rest}"
    return path.as_uri()


def getattr_by_path(thing: Any, path: str) -> Any:
    # getattr(foo, "bar.baz")
    for p in path.split("."):
        thing = getattr(thing, p)
    return thing
