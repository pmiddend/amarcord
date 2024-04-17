import re
from dataclasses import dataclass
from pathlib import Path
from time import time
from typing import IO

from amarcord.db.attributi import datetime_from_attributo_int
from amarcord.web.json_models import JsonBeamtime


@dataclass(frozen=True, eq=True)
class CrystFELCellFile:
    lattice_type: str
    centering: str
    unique_axis: None | str

    a: float
    b: float
    c: float
    alpha: float
    beta: float
    gamma: float


_cell_description_regex = re.compile(
    r"(triclinic|monoclinic|orthorhombic|tetragonal|rhombohedral|hexagonal|cubic)\s+([PABCIFRH])\s+([abc?*])\s+\(([0-9]*(?:\.[0-9]*)?)\s+([0-9]*(?:\.[0-9]*)?)\s+([0-9]*(?:\.[0-9]*)?)\)\s+\(([0-9]*(?:\.[0-9]*)?)\s+([0-9]*(?:\.[0-9]*)?)\s+([0-9]*(?:\.[0-9]*)?)\)"
)


def coparse_cell_description(s: CrystFELCellFile) -> str:
    ua = s.unique_axis if s.unique_axis is not None else "?"
    return f"{s.lattice_type} {s.centering} {ua} ({s.a} {s.b} {s.c}) ({s.alpha} {s.beta} {s.gamma})"


def parse_cell_description(s: str) -> None | CrystFELCellFile:
    match = _cell_description_regex.fullmatch(s)
    if match is None:
        return None
    unique_axis = match.group(3)
    return CrystFELCellFile(
        lattice_type=match.group(1),
        centering=match.group(2),
        unique_axis=None if unique_axis == "?" else unique_axis,
        a=float(match.group(4)),
        b=float(match.group(5)),
        c=float(match.group(6)),
        alpha=float(match.group(7)),
        beta=float(match.group(8)),
        gamma=float(match.group(9)),
    )


def coparse_cell_file(c: CrystFELCellFile, f: IO[str]) -> None:
    f.write("CrystFEL unit cell file version 1.0\n\n")
    f.write(f"lattice_type = {c.lattice_type}\n")
    f.write(f"centering = {c.centering}\n")
    if c.unique_axis is not None:
        f.write(f"unique_axis = {c.unique_axis}\n\n")
    f.write(f"a = {c.a} A\n")
    f.write(f"b = {c.b} A\n")
    f.write(f"c = {c.c} A\n")
    f.write(f"al = {c.alpha} deg\n")
    f.write(f"be = {c.beta} deg\n")
    f.write(f"ga = {c.gamma} deg\n")


def write_cell_file(c: CrystFELCellFile, p: Path) -> None:
    with p.open("w") as f:
        coparse_cell_file(c, f)


def make_cell_file_name(c: CrystFELCellFile) -> str:
    ua = c.unique_axis if c.unique_axis else "noaxis"
    return f"chemical_{c.lattice_type}_{c.centering}_{ua}_{c.a}_{c.b}_{c.c}_{c.alpha}_{c.beta}_{c.gamma}_{int(time())}.cell"


def determine_output_directory(
    beamtime: JsonBeamtime,
    base_directory_template: Path,
    additional_replacements: dict[str, str],
) -> Path:
    # This is a gratuitous selection of beamtime metadata. Please add necessary fields if you need them.
    job_base_directory_str = (
        str(base_directory_template)
        .replace("{beamtime.external_id}", beamtime.external_id)
        .replace(
            "{beamtime.year}", str(datetime_from_attributo_int(beamtime.start).year)
        )
        .replace("{beamtime.beamline}", beamtime.beamline)
        .replace("{beamtime.beamline_lowercase}", beamtime.beamline.lower())
    )

    for k, v in additional_replacements.items():
        job_base_directory_str = job_base_directory_str.replace("{" + k + "}", v)

    if "{" in job_base_directory_str or "}" in job_base_directory_str:
        raise Exception(
            f"job base directory {job_base_directory_str} contains placeholder characters (so '{{' and '}}'), stopping"
        )

    return Path(job_base_directory_str)
