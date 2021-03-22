import random
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


def random_from_range(r: NumericRange) -> float:
    left = r.minimum if r.minimum is not None else r.minimum_inclusive
    right = r.maximum if r.maximum is not None else r.maximum_inclusive
    if left is not None and right is not None:
        return random.uniform(left, right)
    if left is None and right is None:
        return random.uniform(-10000, 10000)
    if left is not None and right is None:
        return random.uniform(left, left + 10000)
    return random.uniform(right - 10000, right)


def empty_range() -> NumericRange:
    return NumericRange(None, False, None, False)
