import contextlib
import sys
from io import BytesIO, StringIO
from typing import Literal, TypeVar

import pandas as pd
from pydantic import BaseModel, field_validator
from smart_open import open

from .types import Kwargs, PathLike

IO = TypeVar("IO", bound=StringIO | BytesIO)


class Handler(BaseModel):
    name: str
    kwargs: Kwargs = {}

    @field_validator("name")
    def validate_handler(cls, v):
        handler = getattr(pd, v, getattr(pd.DataFrame, v, None))
        if handler is None:
            raise ValueError("Unknown handler: `%s`" % v)
        return v

    def handle(self, io: IO, df: pd.DataFrame | None = None) -> pd.DataFrame | None:
        if df is not None:
            handler = getattr(df, self.name)
        else:
            handler = getattr(pd, self.name)
        return handler(io, **self.kwargs)


DefaultReadHandler = Handler(name="read_csv")
DefaultWriteHandler = Handler(name="to_csv")


def read_pandas(
    uri: PathLike | IO,
    mode: str | None = "rb",
    handler: Handler | None = DefaultReadHandler,
) -> pd.DataFrame:
    with smart_open(uri, sys.stdin.buffer, mode=mode) as io:
        return handler.handle(io)


def write_pandas(
    uri: PathLike | IO,
    df: pd.DataFrame,
    mode: str | None = "wb",
    handler: Handler | None = DefaultWriteHandler,
) -> None:
    with smart_open(uri, sys.stdout.buffer, mode=mode) as io:
        return handler.handle(io, df)


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
