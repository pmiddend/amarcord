from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True, eq=True)
class NumericRange:
    minimum: Optional[float]
    minimum_inclusive: bool
    maximum: Optional[float]
    maximum_inclusive: bool

    def __repr__(self) -> str:
        s = "[" if self.minimum_inclusive else "("
        s += str(self.minimum) if self.minimum is not None else "oo"
        s += ","
        s += str(self.maximum) if self.maximum is not None else "oo"
        return s + ("]" if self.maximum_inclusive else ")")

    def value_is_inside(self, v: float) -> bool:
        if self.minimum is not None and (
            v < self.minimum or v == self.minimum and not self.minimum_inclusive
        ):
            return False
        if self.maximum is not None and (
            v > self.maximum or v == self.maximum and not self.maximum_inclusive
        ):
            return False
        return True


def empty_range() -> NumericRange:
    return NumericRange(None, False, None, False)
