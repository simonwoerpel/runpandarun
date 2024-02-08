import sys
from pathlib import Path
from typing import Any, BinaryIO, TextIO, TypeAlias
from urllib.parse import urlparse

import fsspec
import orjson
import pandas as pd
from pantomime import types
from pydantic import BaseModel, ConfigDict, field_validator

from runpandarun.exceptions import SpecError
from runpandarun.types import PathLike, SDict
from runpandarun.util import guess_mimetype

Uri: TypeAlias = Path | BinaryIO | TextIO | str


class Handler(BaseModel):
    options: SDict | None = {}
    uri: Uri | None = "-"
    handler: str | None = None
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    @field_validator("handler")
    @classmethod
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

    def handle(self, uri: Uri | None = None) -> pd.DataFrame:
        uri = uri or self.uri
        return read_pandas(uri, self.get_name(), **self.options)


class WriteHandler(Handler):
    _default_handler = "to_csv"

    def handle(self, df: pd.DataFrame, uri: Uri | None = None) -> None:
        uri = uri or self.uri
        return write_pandas(df, uri, self.get_name(), **self.options)


def read_pandas(
    uri: Uri,
    handler: str | None = "read_csv",
    **kwargs,
) -> pd.DataFrame:
    if uri == "-":
        uri = sys.stdin.buffer
    arg, kwargs = get_pandas_kwargs(handler, uri, **kwargs)
    handler = getattr(pd, handler)
    res = handler(arg, **kwargs)
    return res


def write_pandas(
    df: pd.DataFrame,
    uri: Uri,
    handler: str | None = "to_csv",
    **kwargs,
) -> None:
    if uri == "-":
        uri = sys.stdout.buffer
    arg, kwargs = get_pandas_kwargs(handler, uri, **kwargs)
    handler = getattr(df, handler)
    res = handler(arg, **kwargs)
    return res


def read_json(uri: Uri) -> Any:
    if hasattr(uri, "read"):  # TextIOWrapper
        return orjson.loads(uri.read())
    with fsspec.open(uri) as f:
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


def get_pandas_kwargs(handler: str, uri: Uri, **kwargs) -> tuple[Any, dict[str, Any]]:
    """
    Try to align our Spec with `uri` param to pandas api.
    """
    arg = uri
    if handler == "json_normalize":
        arg = read_json(uri)
    elif "sql" in handler:
        arg = kwargs.pop("sql", None)
        if not isinstance(arg, str):
            raise SpecError("Provide `sql` parameter: A table name or SQL query")
        kwargs["con"] = uri
    return arg, kwargs
