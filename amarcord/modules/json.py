from typing import Dict, List, Union

# Recursive types aren't supported in mypy, see
# https://github.com/python/mypy/issues/731
JSONArray = List["JSONValue"]  # type: ignore
JSONValue = Union[int, str, float, bool, None, "JSONDict", "JSONArray"]  # type: ignore
JSONDict = Dict[str, JSONValue]  # type: ignore
