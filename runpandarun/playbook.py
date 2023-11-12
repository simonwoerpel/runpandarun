from pathlib import Path
from typing import Any, TypeVar

import yaml
from pandas import DataFrame, Series
from pydantic import BaseModel, ConfigDict, model_validator

from .datapatch import Patches, apply_patches
from .exceptions import SpecError
from .io import ReadHandler, WriteHandler
from .types import PathLike
from .util import absolute_path_uri, expandvars, getattr_by_path, safe_eval

P = TypeVar("P", bound="Playbook")


class ExpandMixin:
    def __init__(self, **data):
        super().__init__(**expandvars(data))


MODULES = {
    "DataFrame": DataFrame,
    "Series": Series,
}


class Operation(ExpandMixin, BaseModel):
    handler: str
    options: dict[str, Any] | None = {}
    column: str | None = None
    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="before")
    def validate_handler(cls, values):
        try:
            handler = values["handler"]
            module, func = handler.split(".", 1)
        except Exception as e:
            raise SpecError(f"Invalid handler provided: `{e}`")
        if module not in ("DataFrame", "Series"):
            raise SpecError(f"`{module}` is not any of `DataFrame` or `Series`")
        if module == "Series" and values.get("column") is None:
            raise SpecError(
                f"Provide a `column` parameter when using the `{handler}` handler."
            )
        try:
            getattr_by_path(MODULES[module], func)
        except Exception as e:
            raise SpecError(f"Could not load function `{handler}`: {e}")
        return values

    def apply(self, df: DataFrame) -> DataFrame:
        options = {}
        for key, value in self.options.items():
            if key == "func" or (isinstance(value, str) and value.startswith("lambda")):
                options[key] = safe_eval(value)
            else:
                options[key] = value
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
    patch: Patches | None = None
    write: WriteHandler | None = WriteHandler()
    model_config = ConfigDict(extra="forbid")

    def run(self, df: DataFrame | None = None, write: bool | None = False) -> DataFrame:
        if df is None:
            df = self.read.handle()

        for op in self.operations:
            df = op.apply(df)

        if self.patch:
            df = apply_patches(self.patch, df)

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
