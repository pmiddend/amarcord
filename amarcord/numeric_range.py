import random
from dataclasses import dataclass


@dataclass(frozen=True, eq=True)
class NumericRange:
    minimum: float | None
    minimum_inclusive: bool
    maximum: float | None
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
    left = r.minimum if r.minimum is not None else None
    right = r.maximum if r.maximum is not None else None
    if left is not None and right is not None:
        return random.uniform(left, right)
    if left is None and right is None:
        return random.uniform(-10000, 10000)
    if left is not None and right is None:
        return random.uniform(left, left + 10000)
    # mypy doesn't get it! Maybe some day.
    return random.uniform(right - 10000, right)  # type: ignore


def empty_range() -> NumericRange:
    return NumericRange(None, False, None, False)
