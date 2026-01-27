from typing import Mapping
from typing import Sequence

# Recursive types aren't supported in mypy, see
# https://github.com/python/mypy/issues/731
type JSONArray = Sequence["JSONValue"]  # type: ignore
type JSONValue = int | str | float | bool | None | "JSONDict" | "JSONArray"  # type: ignore
type JSONDict = Mapping[str, JSONValue]  # type: ignore
