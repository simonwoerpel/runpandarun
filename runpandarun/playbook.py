from typing import Any, TypeAlias, TypeVar

import pandas as pd
import yaml
from banal import clean_dict
from pydantic import BaseModel

from . import logic
from .io import Handler, read_pandas, write_pandas
from .logic.columns import Column
from .logic.ops import Operation
from .types import PathLike
from .util import absolute_path, expandvars

P = TypeVar("P", bound="Playbook")

ODict: TypeAlias = dict[str, Any] | None


class RequestParams(BaseModel):
    params: ODict = None
    header: ODict | None = None


class Playbook(BaseModel):
    in_uri: str | None = "-"
    out_uri: str | None = "-"
    handler: Handler | None = Handler(name="read_csv")
    request: RequestParams | None = None
    columns: list[Column] | None = None
    index: str | None = None
    dt_index: str | dict[str, Any] | None = None
    ops: list[Operation] | None = None

    def __init__(self, **data):
        super().__init__(**expandvars(data))

    def run(self, write: bool | None = True) -> pd.DataFrame:
        df = read_pandas(self.in_uri, handler=self.handler)
        if self.columns:
            df = logic.apply_columns(df, self.columns)
        if self.ops:
            df = logic.apply_ops(df, self.ops)
        if self.index or self.dt_index:
            df = logic.apply_index(df, self)
        if write:
            write_pandas(self.out_uri, df)
        return df

    def merge(self, **data) -> P:
        return self.__class__(**{**self.dict(), **clean_dict(data)})

    @classmethod
    def from_yaml(cls, path: PathLike) -> P:
        with open(path) as fh:
            data = yaml.safe_load(fh)
        play = cls(**data)
        play.in_uri = absolute_path(play.in_uri, path)
        play.out_uri = absolute_path(play.out_uri, path)
        return play

    @classmethod
    def from_string(cls, data: str) -> P:
        return cls(**yaml.safe_load(data))
