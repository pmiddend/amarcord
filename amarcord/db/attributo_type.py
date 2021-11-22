from dataclasses import dataclass
from typing import Any
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

from amarcord.numeric_range import NumericRange


@dataclass(frozen=True)
class AttributoTypeInt:
    nonNegative: bool = False
    range: Optional[Tuple[int, int]] = None


@dataclass(frozen=True)
class AttributoTypeDuration:
    pass


@dataclass(frozen=True)
class AttributoTypeList:
    sub_type: "AttributoType"
    min_length: Optional[int]
    max_length: Optional[int]


@dataclass(frozen=True)
class AttributoTypeString:
    pass


@dataclass(frozen=True)
class AttributoTypeUserName:
    pass


@dataclass(frozen=True)
class AttributoTypeComments:
    pass


@dataclass(frozen=True)
class AttributoTypeDouble:
    range: Optional[NumericRange] = None
    suffix: Optional[str] = None
    standard_unit: bool = False


@dataclass(frozen=True)
class AttributoTypeTags:
    pass


@dataclass(frozen=True)
class AttributoTypeSample:
    pass


@dataclass(frozen=True)
class AttributoTypeDateTime:
    pass


@dataclass(frozen=True)
class AttributoTypePath:
    pass


@dataclass(frozen=True)
class AttributoTypeChoice:
    values: List[Tuple[str, Any]]


AttributoType = Union[
    AttributoTypeInt,
    AttributoTypeChoice,
    AttributoTypeDouble,
    AttributoTypeTags,
    AttributoTypeSample,
    AttributoTypeString,
    AttributoTypeComments,
    AttributoTypePath,
    AttributoTypeDateTime,
    AttributoTypeDuration,
    AttributoTypeUserName,
    AttributoTypeList,
]
