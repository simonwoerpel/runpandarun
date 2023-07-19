from pathlib import Path
from typing import Any, TypeVar

import yaml
from pandas import DataFrame, Series
from pydantic import BaseModel
from pydantic import validator as field_validator

from .exceptions import SpecError
from .io import ReadHandler, WriteHandler
from .util import PathLike, absolute_path_uri, expandvars, getattr_by_path, safe_eval

P = TypeVar("P", bound="Playbook")


class ExpandMixin:
    def __init__(self, **data):
        super().__init__(**expandvars(data))


MODULES = {
    "DataFrame": DataFrame,
    "Series": Series,
}


class Operation(ExpandMixin, BaseModel):
    options: dict[str, Any] | None = {}
    handler: str
    column: str | None = None

    @field_validator("handler")
    def validate_handler(cls, v):
        module, func = v.split(".", 1)
        if module not in ("DataFrame", "Series"):
            raise SpecError(f"`{module}` is not any of `DataFrame` or `Series`")
        try:
            getattr_by_path(MODULES[module], func)
        except Exception as e:
            raise SpecError(f"Could not load function `{v}`: {e}")
        return v

    def apply(self, df: DataFrame) -> DataFrame:
        options = {k: safe_eval(v) for k, v in self.options.items()}
        _, func = self.handler.split(".", 1)
        if self.column:
            func = getattr_by_path(df[self.column], func)
            df[self.column] = func(**options)
        else:
            func = getattr_by_path(df, func)
            df = func(**options)
        return df


class Playbook(ExpandMixin, BaseModel):
    read: ReadHandler | None = ReadHandler()
    operations: list[Operation] | None = []
    write: WriteHandler | None = WriteHandler()

    def run(self, df: DataFrame | None = None, write: bool | None = False) -> DataFrame:
        if df is None:
            df = self.read.handle()

        for op in self.operations:
            df = op.apply(df)

        if write:
            self.write.handle(df)
        return df

    @classmethod
    def from_yaml(cls, path: PathLike) -> P:
        path = Path(path)
        with open(path) as fh:
            data = yaml.safe_load(fh)
        play = cls(**data)
        play.read.uri = absolute_path_uri(play.read.uri, path.parent)
        play.write.uri = absolute_path_uri(play.write.uri, path.parent)
        return play

    @classmethod
    def from_string(cls, data: str) -> P:
        return cls(**yaml.safe_load(data))
