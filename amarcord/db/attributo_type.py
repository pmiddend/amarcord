from dataclasses import dataclass
from typing import List
from typing import Optional
from typing import Union

from amarcord.numeric_range import NumericRange


@dataclass(frozen=True)
class AttributoTypeInt:
    def __str__(self) -> str:
        return "integer"


@dataclass(frozen=True)
class AttributoTypeDuration:
    def __str__(self) -> str:
        return "duration"


@dataclass(frozen=True)
class AttributoTypeList:
    sub_type: "AttributoType"
    min_length: Optional[int]
    max_length: Optional[int]

    def __str__(self) -> str:
        if self.min_length is None and self.max_length is None:
            return f"list of {self.sub_type}"
        if self.min_length is not None and self.max_length is not None:
            return f"list (between {self.min_length} and {self.max_length}) of {self.sub_type}"
        if self.min_length is not None:
            return f"list (at least {self.min_length} element(s)) of {self.sub_type}"
        return f"list (at most {self.max_length} element(s)) of {self.sub_type}"


@dataclass(frozen=True)
class AttributoTypeString:
    def __str__(self) -> str:
        return "string"


@dataclass(frozen=True)
class AttributoTypeComments:
    def __str__(self) -> str:
        return "list of comments"


@dataclass(frozen=True)
class AttributoTypeDouble:
    range: Optional[NumericRange] = None
    suffix: Optional[str] = None
    standard_unit: bool = False

    def __str__(self) -> str:
        if self.range is None and self.suffix is None:
            return "decimal number"

        if self.suffix is not None and self.range is not None:
            return f"{self.suffix} as decimal number in {repr(self.range)}"

        if self.suffix is not None:
            return f"{self.suffix} as decimal number"

        return f"decimal number in {repr(self.range)}"


@dataclass(frozen=True)
class AttributoTypeSample:
    def __str__(self) -> str:
        return "sample"


@dataclass(frozen=True)
class AttributoTypeDateTime:
    def __str__(self) -> str:
        return "date and time"


@dataclass(frozen=True)
class AttributoTypeChoice:
    values: List[str]

    def __str__(self) -> str:
        return "one of: " + ",".join(self.values)


AttributoType = Union[
    AttributoTypeInt,
    AttributoTypeChoice,
    AttributoTypeDouble,
    AttributoTypeSample,
    AttributoTypeString,
    AttributoTypeComments,
    AttributoTypeDateTime,
    AttributoTypeDuration,
    AttributoTypeList,
]
