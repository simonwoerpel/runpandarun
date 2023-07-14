import os
from typing import Any, TypeAlias

Kwargs: TypeAlias = dict[str, Any] | None
PathLike: TypeAlias = str | os.PathLike[str]
