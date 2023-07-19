import sys
from io import BytesIO, StringIO
from typing import Any, TypeVar

import fsspec
import orjson
import pandas as pd
from pantomime import types
from pydantic import BaseModel
from pydantic import validator as field_validator

from .util import PathLike, guess_mimetype

IO = TypeVar("IO", bound=StringIO | BytesIO)


class Handler(BaseModel):
    options: dict[str, Any] | None = {}
    uri: str | None = "-"
    handler: str | None = None

    @field_validator("handler")
    def validate_handler(cls, v):
        if v is not None:
            handler = getattr(pd, v, None)
            if handler is None:
                raise ValueError("Unknown handler: `%s`" % v)
        return v

    def get_handler_name(self) -> str:
        if self.handler is not None:
            return self.handler
        if self.uri is not None and self.uri != "-":
            handler = guess_handler(self.uri)
            if "write" in self.__class__.__name__.lower():
                return f"to_{handler}"
            return f"read_{handler}"
        return self._default_handler


class ReadHandler(Handler):
    _default_handler = "read_csv"

    def handle(self, io: IO | str | None = None) -> pd.DataFrame:
        io = io or self.uri
        return read_pandas(io, self.get_handler_name(), **self.options)


class WriteHandler(Handler):
    _default_handler = "to_csv"

    def handle(self, df: pd.DataFrame, io: IO | str | None = None) -> None:
        io = io or self.uri
        return write_pandas(df, io, self.get_handler_name(), **self.options)


def read_pandas(
    io: PathLike | IO,
    handler: str | None = "read_csv",
    mode: str | None = "rb",
    **kwargs,
) -> pd.DataFrame:
    if io == "-":
        io = sys.stdin.buffer
    if handler == "json_normalize":
        io = read_json(io)
    handler = getattr(pd, handler)
    res = handler(io, **kwargs)
    if hasattr(io, "close"):
        io.close()
    return res


def write_pandas(
    df: pd.DataFrame,
    io: PathLike | IO,
    handler: str | None = "to_csv",
    mode: str | None = "wb",
    **kwargs,
) -> None:
    handler = getattr(df, handler)
    if io == "-":
        io = sys.stdout.buffer
    res = handler(io, **kwargs)
    if hasattr(io, "close"):
        io.close()
    return res


def read_json(io: PathLike | IO) -> Any:
    with fsspec.open(io) as f:
        data = f.read()
    return orjson.loads(data)


def guess_handler(path: PathLike) -> str:
    mimetype = guess_mimetype(path)
    if mimetype == types.CSV:
        return "csv"
    if mimetype in (types.EXCEL, types.XLS, types.XLSX):
        return "excel"
    if mimetype == types.JSON:
        return "json"
    if mimetype in (types.XML, "application/xml"):  # FIXME pantomime
        return "xml"
    if mimetype == types.HTML:
        return "html"
    raise NotImplementedError(f"Please specify pandas handler for type `{mimetype}`")
