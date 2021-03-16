from dataclasses import dataclass
from typing import Any, List, Optional, Tuple, Union

from amarcord.numeric_range import NumericRange


@dataclass(frozen=True)
class PropertyInt:
    nonNegative: bool = False
    range: Optional[Tuple[int, int]] = None


@dataclass(frozen=True)
class PropertyDuration:
    pass


@dataclass(frozen=True)
class PropertyList:
    sub_property: "RichAttributoType"
    min_length: Optional[int]
    max_length: Optional[int]


@dataclass(frozen=True)
class PropertyString:
    pass


@dataclass(frozen=True)
class PropertyUserName:
    pass


@dataclass(frozen=True)
class PropertyComments:
    pass


@dataclass(frozen=True)
class PropertyDouble:
    range: Optional[NumericRange] = None
    suffix: Optional[str] = None


@dataclass(frozen=True)
class PropertyTags:
    pass


@dataclass(frozen=True)
class PropertySample:
    pass


@dataclass(frozen=True)
class PropertyDateTime:
    pass


@dataclass(frozen=True)
class PropertyChoice:
    values: List[Tuple[str, Any]]


RichAttributoType = Union[
    PropertyInt,
    PropertyChoice,
    PropertyDouble,
    PropertyTags,
    PropertySample,
    PropertyString,
    PropertyComments,
    PropertyDateTime,
    PropertyDuration,
    PropertyUserName,
    PropertyList,
]
