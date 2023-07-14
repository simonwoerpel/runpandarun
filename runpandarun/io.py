import contextlib
import sys
from io import BytesIO, StringIO
from typing import Any, Literal, TypeVar

import pandas as pd
from pydantic import BaseModel, field_validator
from smart_open import open

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
        return write_pandas(io, df, self.handler, **self.options)


def read_pandas(
    uri: PathLike | IO,
    handler: str | None = "read_csv",
    mode: str | None = "rb",
    **kwargs
) -> pd.DataFrame:
    handler = getattr(pd, handler)
    with smart_open(uri, sys.stdin.buffer, mode=mode) as io:
        return handler(io, **kwargs)


def write_pandas(
    uri: PathLike | IO,
    df: pd.DataFrame,
    handler: str | None = "to_csv",
    mode: str | None = "wb",
    **kwargs
) -> None:
    handler = getattr(df, handler)
    with smart_open(uri, sys.stdout.buffer, mode=mode) as io:
        return handler(io, **kwargs)


@contextlib.contextmanager
def smart_open(
    uri: PathLike | IO | None = None,
    sys_io: Literal[sys.stdin.buffer, sys.stdout.buffer] | None = sys.stdin,
    **kwargs
):
    """
    smart_open plus stdin/stdout
    """
    if isinstance(uri, (StringIO, BytesIO)):
        fh = uri
    elif uri and uri != "-":
        fh = open(uri, **kwargs)
    else:
        fh = sys_io

    try:
        yield fh
    finally:
        if fh not in (sys.stdout.buffer, sys.stdin.buffer):
            fh.close()
