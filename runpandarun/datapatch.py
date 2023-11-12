from functools import cache, lru_cache, partial
from typing import Any, TypeAlias

import pandas as pd
from datapatch.lookup import Lookup
from pydantic import BaseModel

from .types import SDict


class Datapatch(BaseModel):
    required: bool | None = False
    normalize: bool | None = False
    map: SDict | None = dict()
    options: list[SDict] | None = []

    def __hash__(self) -> int:
        return hash(str(repr(self.model_dump())))


Patches: TypeAlias = dict[str, Datapatch]


@cache
def get_lookup(name: str, patch: Datapatch) -> Lookup:
    return Lookup(name=name, config=patch.model_dump())


@lru_cache(maxsize=10_000)
def apply_patch(value: Any, patch: Datapatch, column: str) -> SDict:
    if pd.isna(value) or not value:
        return value
    lookup = get_lookup(column, patch)
    return lookup.get_value(value, default=value)


def apply_patches(patches: Patches, df: pd.DataFrame) -> SDict:
    for column, patch in patches.items():
        if column in df.columns:
            apply = partial(apply_patch, patch=patch, column=column)
            df[column] = df[column].map(apply)
    return df
