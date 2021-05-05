import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Set
from typing import Union

from pint import UnitRegistry
from pint.quantity import Quantity


@dataclass(frozen=True)
class P11InfoFile:
    run_type: str
    run_name: str
    start_angle: Quantity
    frames: int
    degrees_per_frame: Quantity
    exposure_time: Quantity
    energy: Quantity
    wavelength: Quantity
    detector_distance: Quantity
    resolution: Quantity
    aperture: Quantity
    focus: str
    filter_transmission_percent: float
    filter_thickness: Quantity
    ring_current: Quantity


def parse_p11_info_file(fn: Path, ureg: UnitRegistry) -> P11InfoFile:
    strings: Set[str] = {"run type", "run name", "focus"}
    ints: Set[str] = {"frames"}
    # info.txt uses "A" for ampere and angstroms interchangeable (bad info.txt, bad!)
    angstroms: Set[str] = {"resolution", "wavelength"}
    result: Dict[str, Union[str, int, Quantity]] = {}
    with fn.open("r") as f:
        line_regex = re.compile(r"([^:]+):\s*(.+)")
        for line_no, line in enumerate(f):
            # First newline is the delimiter between key/value and this "focus/beam area/flux" table (I think)
            if not line.strip():
                break

            match = re.fullmatch(line_regex, line.strip())

            if not match:
                raise Exception(
                    f'"{fn}":{line_no}: invalid line (should be $name: $value, is {line})'
                )

            key = match.group(1)
            value = match.group(2)

            if key in strings:
                if not isinstance(value, str):
                    raise Exception(
                        f'"{fn}":{line_no}: {key} should be string, is {type(value)}: {value}'
                    )
                result[key] = value
            elif key in ints:
                try:
                    result[key] = int(value)
                except:
                    raise Exception(
                        f'"{fn}":{line_no}: {key} should be integer, is {type(value)}: {value}'
                    )
            elif key in angstroms:
                try:
                    result[key] = float(value[0:-1]) * ureg("angstrom")
                except:
                    raise Exception(
                        f'"{fn}":{line_no}: {key} should be a floating point angstrom value, is {type(value)}: {value}'
                    )
            else:
                try:
                    result[key] = ureg(value)
                except:
                    raise Exception(
                        f'"{fn}":{line_no}: {key} should be numeric quantity, is: {value}'
                    )

    def get_safely(result: Dict[str, Union[str, int, Quantity]], v: str) -> Any:
        if v not in result:
            raise Exception(f"{fn}: couldn't find {v}")
        return result[v]

    return P11InfoFile(
        run_type=get_safely(result, "run type"),
        run_name=get_safely(result, "run name"),
        start_angle=get_safely(result, "start angle"),
        frames=get_safely(result, "frames"),
        degrees_per_frame=get_safely(result, "degrees/frame"),
        exposure_time=get_safely(result, "exposure time"),
        energy=get_safely(result, "energy"),
        wavelength=get_safely(result, "wavelength"),
        detector_distance=get_safely(result, "detector distance"),
        resolution=get_safely(result, "resolution"),
        aperture=get_safely(result, "aperture"),
        focus=get_safely(result, "focus"),
        filter_transmission_percent=get_safely(result, "filter transmission"),
        filter_thickness=get_safely(result, "filter thickness"),
        ring_current=get_safely(result, "ring current"),
    )
