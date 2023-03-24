import re
from dataclasses import dataclass

from pint import Quantity
from pint import UnitRegistry


@dataclass(frozen=True)
class Polarisation:
    angle: Quantity
    percentage: int


_CRYSTFEL_POLARISATION_PREDEFINED_REGEX = re.compile(r"(horiz|vert)([0-9]+)?")
_CRYSTFEL_POLARISATION_SPECIFIC_REGEX = re.compile(r"[0-9]+deg([0-9]+)?")
_UNIT_REGISTRY = UnitRegistry()


class PolarisationError(str):
    pass


def parse_crystfel_polarisation(s: str) -> PolarisationError | None | Polarisation:
    if s == "none":
        return None
    predefined_match = _CRYSTFEL_POLARISATION_PREDEFINED_REGEX.match(s)
    if predefined_match is not None:
        return Polarisation(
            angle=(
                0 if predefined_match.group(1) == "horiz" else 90
            )  # pyright: ignore [reportUnknownArgumentType]
            * _UNIT_REGISTRY.degrees,
            percentage=int(predefined_match.group(2))
            if predefined_match.group(2) is not None
            else 100,
        )
    specific_match = _CRYSTFEL_POLARISATION_SPECIFIC_REGEX.match(s)
    if specific_match is None:
        return PolarisationError(
            'couldn\'t parse polarisation string "{s}"; it seems to be neither of the form "horiz|vert", '
            + 'then an optional integral percentage, nor "number", then "deg", then an optional integral percentage'
        )
    return Polarisation(
        angle=int(
            specific_match.group(1)
        )  # pyright: ignore [reportUnknownArgumentType]
        * _UNIT_REGISTRY.degrees,
        percentage=int(specific_match.group(2))
        if specific_match.group(2) is not None
        else 100,
    )
