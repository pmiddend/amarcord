from typing import Mapping
from typing import Sequence
from typing import TypeAlias
from typing import Union

# Recursive types aren't supported in mypy, see
# https://github.com/python/mypy/issues/731
JSONArray: TypeAlias = Sequence["JSONValue"]  # type: ignore
JSONValue: TypeAlias = Union[int, str, float, bool, None, "JSONDict", "JSONArray"]  # type: ignore
JSONDict: TypeAlias = Mapping[str, JSONValue]  # type: ignore
