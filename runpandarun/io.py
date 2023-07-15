import sys
from io import BytesIO, StringIO
from typing import Any, TypeVar

import pandas as pd
from pydantic import BaseModel, field_validator

from .util import PathLike

IO = TypeVar("IO", bound=StringIO | BytesIO)


class Handler(BaseModel):
    options: dict[str, Any] | None = {}
    uri: str | None = "-"


class ReadHandler(Handler):
    handler: str | None = "read_csv"

    @field_validator("handler")
    def validate_handler(cls, v):
        handler = getattr(pd, v, None)
        if handler is None:
            raise ValueError("Unknown handler: `%s`" % v)
        return v

    def handle(self, io: IO | str | None = None) -> pd.DataFrame:
        io = io or self.uri
        return read_pandas(io, self.handler, **self.options)


class WriteHandler(Handler):
    handler: str | None = "to_csv"

    @field_validator("handler")
    def validate_handler(cls, v):
        handler = getattr(pd.DataFrame, v, None)
        if handler is None:
            raise ValueError("Unknown handler: `%s`" % v)
        return v

    def handle(self, df: pd.DataFrame, io: IO | str | None = None) -> None:
        io = io or self.uri
        return write_pandas(df, io, self.handler, **self.options)


def read_pandas(
    io: PathLike | IO,
    handler: str | None = "read_csv",
    mode: str | None = "rb",
    **kwargs
) -> pd.DataFrame:
    handler = getattr(pd, handler)
    if io == "-":
        io = sys.stdin.buffer
    res = handler(io, **kwargs)
    if hasattr(io, "close"):
        io.close()
    return res


def write_pandas(
    df: pd.DataFrame,
    io: PathLike | IO,
    handler: str | None = "to_csv",
    mode: str | None = "wb",
    **kwargs
) -> None:
    handler = getattr(df, handler)
    if io == "-":
        io = sys.stdout.buffer
    res = handler(io, **kwargs)
    if hasattr(io, "close"):
        io.close()
    return res
