from dataclasses import dataclass
from enum import Enum
from typing import TypeAlias

from amarcord.numeric_range import NumericRange


@dataclass(frozen=True)
class AttributoTypeInt:
    def __str__(self) -> str:
        return "integer"


class ArrayAttributoType(str, Enum):
    ARRAY_STRING = "string"
    ARRAY_BOOL = "bool"
    ARRAY_NUMBER = "number"


@dataclass(frozen=True)
class AttributoTypeList:
    sub_type: ArrayAttributoType
    min_length: int | None
    max_length: int | None

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
class AttributoTypeBoolean:
    def __str__(self) -> str:
        return "boolean"


@dataclass(frozen=True)
class AttributoTypeDecimal:
    range: NumericRange | None = None
    suffix: str | None = None
    standard_unit: bool = False
    tolerance_is_absolute: bool = False
    tolerance: float | None = None

    def __str__(self) -> str:
        tolerance_string = (
            (
                " ("
                + ("absolute " if self.tolerance_is_absolute else "")
                + f"tolerance {self.tolerance})"
            )
            if self.tolerance is not None
            else ""
        )
        if self.range is None and self.suffix is None:
            return f"decimal number{tolerance_string}"

        if self.suffix is not None and self.range is not None:
            return (
                f"{self.suffix} as decimal number in {self.range!r}{tolerance_string}"
            )

        if self.suffix is not None:
            return f"{self.suffix} as decimal number{tolerance_string}"

        return f"decimal number in {self.range!r}{tolerance_string}"


@dataclass(frozen=True)
class AttributoTypeChemical:
    def __str__(self) -> str:
        return "chemical"


@dataclass(frozen=True)
class AttributoTypeDateTime:
    def __str__(self) -> str:
        return "date and time"


@dataclass(frozen=True)
class AttributoTypeChoice:
    values: list[str]

    def __str__(self) -> str:
        return "one of: " + ",".join(self.values)


AttributoType: TypeAlias = (
    AttributoTypeInt
    | AttributoTypeBoolean
    | AttributoTypeString
    | AttributoTypeChemical
    | AttributoTypeChoice
    | AttributoTypeDecimal
    | AttributoTypeDateTime
    | AttributoTypeList
)
