import mimetypes
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import banal
import fingerprints
import normality
import numpy as np
import pandas as pd
from pantomime import normalize_mimetype

from .types import PathLike


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


def absolute_path(path: PathLike, base: PathLike | None = "") -> PathLike:
    scheme = urlparse(str(path)).scheme
    if path == "-" or scheme:
        return path
    return (Path(base) / Path(path)).absolute()


def absolute_path_uri(path: PathLike, base: PathLike | None = "") -> str:
    path = absolute_path(path, base)
    if isinstance(path, Path):
        return path.as_uri()
    return path


def getattr_by_path(thing: Any, path: str) -> Any:
    # getattr(foo, "bar.baz")
    for p in path.split("."):
        thing = getattr(thing, p)
    return thing


def guess_mimetype(path: PathLike) -> str:
    mimetype, _ = mimetypes.guess_type(path)
    return normalize_mimetype(mimetype)
