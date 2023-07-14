import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, TypeAlias

import banal
import fingerprints
import normality
import numpy as np
import pandas as pd
from smart_open import parse_uri

PathLike: TypeAlias = str | os.PathLike[str]


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


def absolute_path(path: PathLike, base: PathLike) -> PathLike | str:
    if path == "-" or parse_uri(str(path)).scheme != "file":
        return path
    return os.path.normpath((Path(base).parent / Path(path)).absolute())


def getattr_by_path(thing: Any, path: str) -> Any:
    # getattr(foo, "bar.baz")
    for p in path.split("."):
        thing = getattr(thing, p)
    return thing
