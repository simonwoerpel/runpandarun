import sys
from typing import Any
from urllib.parse import urlparse

import fsspec
import orjson
import pandas as pd
from pantomime import types
from pydantic import BaseModel
from pydantic import validator as field_validator

from .exceptions import SpecError
from .types import IO, PathLike
from .util import guess_mimetype


class Handler(BaseModel):
    options: dict[str, Any] | None = {}
    uri: str | None = "-"
    handler: str | None = None

    class Config:
        extra = "forbid"

    @field_validator("handler")
    def validate_handler(cls, v):
        if v is not None:
            handler = getattr(pd, v, None)
            if handler is None:
                raise ValueError("Unknown handler: `%s`" % v)
        return v

    def get_name(self) -> str:
        if self.handler is not None:
            return self.handler
        if self.uri is not None and self.uri != "-":
            handler = guess_handler_from_uri(self.uri)
            if "write" in self.__class__.__name__.lower():
                return f"to_{handler}"
            return f"read_{handler}"
        return self._default_handler


class ReadHandler(Handler):
    _default_handler = "read_csv"

    def handle(self, io: IO | str | None = None) -> pd.DataFrame:
        io = io or self.uri
        return read_pandas(io, self.get_name(), **self.options)


class WriteHandler(Handler):
    _default_handler = "to_csv"

    def handle(self, df: pd.DataFrame, io: IO | str | None = None) -> None:
        io = io or self.uri
        return write_pandas(df, io, self.get_name(), **self.options)


def read_pandas(
    io: PathLike | IO,
    handler: str | None = "read_csv",
    mode: str | None = "rb",
    **kwargs,
) -> pd.DataFrame:
    if io == "-":
        io = sys.stdin.buffer
    arg, kwargs = get_pandas_kwargs(handler, io, **kwargs)
    handler = getattr(pd, handler)
    res = handler(arg, **kwargs)
    return res


def write_pandas(
    df: pd.DataFrame,
    io: PathLike | IO,
    handler: str | None = "to_csv",
    mode: str | None = "wb",
    **kwargs,
) -> None:
    if io == "-":
        io = sys.stdout.buffer
    arg, kwargs = get_pandas_kwargs(handler, io, **kwargs)
    handler = getattr(df, handler)
    res = handler(arg, **kwargs)
    return res


def read_json(io: PathLike | IO) -> Any:
    if hasattr(io, "read"):  # TextIOWrapper
        return orjson.loads(io.read())
    with fsspec.open(io) as f:
        return orjson.loads(f.read())


def guess_handler_from_mimetype(mimetype: str) -> str:
    if mimetype == types.CSV:
        return "csv"
    if mimetype in (types.EXCEL, types.XLS, types.XLSX):
        return "excel"
    if mimetype == types.JSON:
        return "json"
    if mimetype == types.XML:
        return "xml"
    if mimetype == types.HTML:
        return "html"
    raise NotImplementedError(f"Please specify pandas handler for type `{mimetype}`")


def guess_handler_from_uri(uri: PathLike) -> str:
    mimetype = guess_mimetype(uri)
    try:
        return guess_handler_from_mimetype(mimetype)
    except NotImplementedError:
        uri = urlparse(uri)
        if "sql" in uri.scheme:
            return "sql"
        raise NotImplementedError(
            f"Please specify pandas handler for type `{mimetype}` ({uri})"
        )


def get_pandas_kwargs(handler: str, io: IO, **kwargs) -> tuple[Any, dict[str, Any]]:
    """
    Try to align our Spec with `uri` param to pandas api.
    """
    arg = io
    if handler == "json_normalize":
        arg = read_json(io)
    elif "sql" in handler:
        arg = kwargs.pop("sql", None)
        if not isinstance(arg, str):
            raise SpecError("Provide `sql` parameter: A table name or SQL query")
        kwargs["con"] = io
    return arg, kwargs
