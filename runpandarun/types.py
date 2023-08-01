import os
from io import BytesIO, StringIO
from pathlib import Path
from typing import Any, TypeAlias, TypeVar

PathLike: TypeAlias = str | os.PathLike[str] | Path
IO = TypeVar("IO", bound=StringIO | BytesIO)
SDict: TypeAlias = dict[str, Any]

__all__ = [PathLike, IO]
