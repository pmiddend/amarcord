from typing import Dict
from typing import List
from typing import Tuple
from typing import Union

# Recursive types aren't supported in mypy, see
# https://github.com/python/mypy/issues/731
JSONArray = List["JSONValue"]  # type: ignore
JSONValue = Union[int, str, float, bool, None, "JSONDict", "JSONArray"]  # type: ignore
JSONDict = Dict[str, JSONValue]  # type: ignore

ImmutableJSONArray = Tuple["ImmutableJSONValue"]  # type: ignore
ImmutableJSONValue = Union[  # type: ignore
    int, str, float, bool, None, "ImmutableJSONDict", "ImmutableJSONArray"  # type: ignore
]
ImmutableJSONDict = Dict[str, ImmutableJSONValue]  # type: ignore


def json_make_immutable(v: JSONValue) -> ImmutableJSONValue:
    if v is None:
        return None
    if isinstance(v, (int, str, float, bool)):
        return v
    if isinstance(v, list):
        return tuple(json_make_immutable(vs) for vs in v)  # type: ignore
    assert isinstance(v, dict)
    return {k: json_make_immutable(vs) for k, vs in v.items()}
