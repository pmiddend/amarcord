from typing import Mapping
from typing import Sequence
from typing import TypeAlias
from typing import Union

# Recursive types aren't supported in mypy, see
# https://github.com/python/mypy/issues/731
JSONArray: TypeAlias = Sequence["JSONValue"]  # type: ignore
# pylint: disable=consider-alternative-union-syntax
JSONValue: TypeAlias = Union[int, str, float, bool, None, "JSONDict", "JSONArray"]  # type: ignore
JSONDict: TypeAlias = Mapping[str, JSONValue]  # type: ignore

ImmutableJSONArray = tuple["ImmutableJSONValue"]  # type: ignore
# pylint: disable=consider-alternative-union-syntax
ImmutableJSONValue = Union[  # type: ignore
    int, str, float, bool, None, "ImmutableJSONDict", "ImmutableJSONArray"  # type: ignore
]
ImmutableJSONDict = dict[str, ImmutableJSONValue]  # type: ignore


def json_make_immutable(v: JSONValue) -> ImmutableJSONValue:
    if v is None:
        return None
    if isinstance(v, (int, str, float, bool)):
        return v
    if isinstance(v, list):
        # noinspection PydanticTypeChecker,PyTypeChecker
        return tuple(json_make_immutable(vs) for vs in v)
    assert isinstance(v, dict)
    return {k: json_make_immutable(vs) for k, vs in v.items()}
