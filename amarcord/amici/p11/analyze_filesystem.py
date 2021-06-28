import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List
from typing import Optional
from typing import Tuple

from pint import UnitRegistry

from amarcord.amici.p11.parser import P11InfoFile
from amarcord.amici.p11.parser import parse_p11_info_file

logger = logging.getLogger(__name__)

WarnMessage = str


@dataclass(frozen=True)
class P11Run:
    run_id: int
    run_path: Path
    info_file: P11InfoFile
    data_raw_filename_pattern: Optional[str]
    microscope_image_filename_pattern: Optional[str]


@dataclass(frozen=True)
class P11Puck:
    puck_id: str
    position: int

    runs: List[P11Run]


@dataclass(frozen=True)
class P11Target:
    target_name: str
    pucks: List[P11Puck]


@dataclass(frozen=True)
class P11Crystal:
    crystal_id: str
    runs: List[P11Run]


def parse_run(
    crystal_name: str, run_id: int, run_path: Path
) -> Tuple[Optional[P11Run], List[WarnMessage]]:
    info_path = run_path / "info.txt"
    if not info_path.is_file():
        return None, [
            f"crystal {crystal_name}, run {run_id}: info file {info_path} not a file"
        ]
    if not info_path.exists():
        return None, [
            f"crystal {crystal_name}, run {run_id}: info file {info_path} does not exist"
        ]
    try:
        return (
            P11Run(
                run_id,
                run_path,
                parse_p11_info_file(info_path, UnitRegistry()),
                data_raw_filename_pattern=f"{run_path}/*h5"
                if list(run_path.glob("*h5"))
                else f"{run_path}/*cbf"
                if list(run_path.glob("*cbf"))
                else None,
                microscope_image_filename_pattern=f"{run_path}/*jpg"
                if list(run_path.glob("*jpg"))
                else None,
            ),
            [],
        )
    except Exception as e:
        return None, [
            f"crystal {crystal_name}, run {run_id}: error parsing {info_path}: {e}"
        ]


def parse_puck(
    puck_id: str, puck_position: int, puck_path: Path
) -> Tuple[Optional[P11Puck], List[WarnMessage]]:
    runs: List[P11Run] = []
    warnings: List[WarnMessage] = []
    for run_dir in puck_path.iterdir():
        if not run_dir.is_dir():
            continue

        match = re.fullmatch(r"([^_]+)_(?:pos)?(\d+)_(\d+)", run_dir.name)
        if not match:
            warnings.append(
                f"run directory {run_dir.name} doesn't match {puck_id}_pos{puck_position}_ or {puck_id}_{puck_position}_ "
            )
            continue

        if match.group(1) != puck_id or int(match.group(2)) != puck_position:
            warnings.append(
                f"run directory {run_dir.name} doesn't match {puck_id}_pos{puck_position}_ or {puck_id}_{puck_position}_ "
            )
            continue

        run_id = int(match.group(3))

        run, run_warnings = parse_run("from puck", run_id, run_dir)

        if run is not None:
            runs.append(run)

        warnings.extend(run_warnings)

    if not runs:
        warnings.append(f"Puck {puck_path} has no runs!")

    return P11Puck(puck_id, puck_position, runs=runs), warnings


def parse_crystal(crystal_path: Path) -> Tuple[Optional[P11Crystal], List[WarnMessage]]:
    runs: List[P11Run] = []
    warnings: List[WarnMessage] = []
    for run_dir in crystal_path.iterdir():
        if not run_dir.is_dir():
            continue

        if not run_dir.name.startswith(crystal_path.name):
            warnings.append(
                f"crystal {crystal_path.name}: crystal run directory {run_dir} has invalid format, doesn't start with "
                f"the crystal ID "
            )
            continue

        remainder = run_dir.name[len(crystal_path.name) + 1 :]

        try:
            run_id = int(remainder)
        except:
            warnings.append(
                f"crystal {crystal_path.name}: crystal run directory {run_dir} invalid format, couldn't find run ID at the end: {remainder}"
            )
            continue

        run, run_warnings = parse_run(crystal_path.name, run_id, run_dir)

        if run is not None:
            runs.append(run)

        warnings.extend(run_warnings)

    if not runs:
        warnings.append(f"crystal {crystal_path}: no (valid) runs!")

    return P11Crystal(crystal_path.name, runs), warnings


def parse_p11_crystals(
    proposal_path: Path,
) -> Tuple[List[P11Crystal], List[WarnMessage]]:
    """Parses all paths below the proposal path, assuming there are crystals directly below that"""
    if not proposal_path.is_dir():
        raise Exception(f"proposal path {proposal_path} is not a directory!")
    raw_path = proposal_path / "raw"
    if not raw_path.is_dir():
        raise Exception(f"proposal raw data {raw_path} is not a directory!")
    result: List[P11Crystal] = []
    warnings: List[WarnMessage] = []
    for crystal_dir in raw_path.iterdir():
        if not crystal_dir.is_dir():
            continue

        crystal_info, crystal_warnings = parse_crystal(crystal_dir)

        warnings.extend(crystal_warnings)

        if crystal_info is not None:
            result.append(crystal_info)

    return result, warnings
