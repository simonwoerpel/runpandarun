import os
from io import BytesIO, StringIO
from pathlib import Path
from typing import TypeAlias, TypeVar

PathLike: TypeAlias = str | os.PathLike[str] | Path
IO = TypeVar("IO", bound=StringIO | BytesIO)

__all__ = [PathLike, IO]
