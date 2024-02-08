import os
from pathlib import Path
from typing import Any, TypeAlias

PathLike: TypeAlias = str | os.PathLike[str] | Path
SDict: TypeAlias = dict[str, Any]
