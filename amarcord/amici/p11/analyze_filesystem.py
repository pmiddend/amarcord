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


@dataclass(frozen=True)
class P11Run:
    run_id: int
    info_file: P11InfoFile
    data_raw_filename_pattern: Optional[str]
    microscope_image_filename_pattern: Optional[str]
    processed_path: Optional[Path]


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


def parse_run(run_id: int, run_path: Path) -> Tuple[Optional[P11Run], bool]:
    info_path = run_path / "info.txt"
    if not info_path.is_file():
        logger.warning("info file %s not a file", info_path)
        return None, True
    if not info_path.exists():
        logger.warning("info file %s does not exist", info_path)
        return None, True
    try:
        processed = Path(str(run_path.resolve()).replace("/raw/", "/processed/"))
        return (
            P11Run(
                run_id,
                parse_p11_info_file(info_path, UnitRegistry()),
                data_raw_filename_pattern=f"{run_path}/*h5"
                if list(run_path.glob("*h5"))
                else f"{run_path}/*cbf"
                if list(run_path.glob("*cbf"))
                else None,
                microscope_image_filename_pattern=f"{run_path}/*jpg"
                if list(run_path.glob("*jpg"))
                else None,
                processed_path=processed if processed.is_dir() else None,
            ),
            False,
        )
    except:
        logger.exception("error parsing %s", info_path)
        return None, True


def parse_puck(
    puck_id: str, puck_position: int, puck_path: Path
) -> Tuple[Optional[P11Puck], bool]:
    runs: List[P11Run] = []
    has_warnings = False
    for run_dir in puck_path.iterdir():
        if not run_dir.is_dir():
            continue

        match = re.fullmatch(r"([^_]+)_(?:pos)?(\d+)_(\d+)", run_dir.name)
        if not match:
            logger.warning(
                "run directory %s doesn't match %s_pos%s_ or %s_%s_ ",
                run_dir.name,
                puck_id,
                puck_position,
                puck_id,
                puck_position,
            )
            has_warnings = True
            continue

        if match.group(1) != puck_id or int(match.group(2)) != puck_position:
            logger.warning(
                "run directory %s doesn't match %s_pos%s_ or %s_%s_ ",
                run_dir.name,
                puck_id,
                puck_position,
                puck_id,
                puck_position,
            )
            has_warnings = True
            continue

        run_id = int(match.group(3))

        run, run_has_warnings = parse_run(run_id, run_dir)

        if run is not None:
            runs.append(run)

        has_warnings = run_has_warnings or has_warnings

    if not runs:
        logger.warning("Puck %s has no runs!", puck_path)
        has_warnings = True

    return P11Puck(puck_id, puck_position, runs=runs), has_warnings


def parse_crystal(crystal_path: Path) -> Tuple[Optional[P11Crystal], bool]:
    runs: List[P11Run] = []
    has_warnings = False
    for run_dir in crystal_path.iterdir():
        if not run_dir.is_dir():
            continue

        if not run_dir.name.startswith(crystal_path.name):
            logger.warning(
                "crystal run directory %s has invalid format, doesn't start with the crystal ID %s",
                run_dir,
                crystal_path.name,
            )
            has_warnings = True
            continue

        remainder = run_dir.name[len(crystal_path.name) + 1 :]

        try:
            run_id = int(remainder)
        except:
            logger.warning(
                "crystal run directory %s invalid format, couldn't find run ID at the end: %s",
                run_dir,
                remainder,
            )
            has_warnings = True
            continue

        run, run_has_warnings = parse_run(run_id, run_dir)

        if run is not None:
            runs.append(run)

        has_warnings = run_has_warnings or has_warnings

    if not runs:
        logger.warning("crystal %s has no (valid) runs!", crystal_path)
        has_warnings = True

    return P11Crystal(crystal_path.name, runs), has_warnings


def parse_target(target_path: Path) -> Tuple[Optional[P11Target], bool]:
    pucks: List[P11Puck] = []
    has_warnings = False
    for puck_dir in target_path.iterdir():
        if not puck_dir.is_dir():
            continue

        parts = puck_dir.name.split("_")
        if len(parts) != 2:
            logger.warning(
                "puck directory %s invalid format, has %s part(s)", puck_dir, len(parts)
            )
            has_warnings = True
            continue

        match = re.fullmatch(r"([^_]+)_(?:pos)?(\d+)", puck_dir.name)

        if not match:
            logger.warning(
                "puck directory %s has invalid format, has to be ${puckid}_pos${pos} or ${puckid}_${pos}",
                puck_dir,
            )
            has_warnings = True
            continue

        puck_id = match.group(1)
        puck_position = int(match.group(2))

        puck, puck_has_warnings = parse_puck(puck_id, puck_position, puck_dir)

        if puck is not None:
            pucks.append(puck)

        has_warnings = has_warnings or puck_has_warnings
    if not pucks:
        logger.warning("target %s has no pucks!", target_path)
        has_warnings = True
    return P11Target(target_name=target_path.name, pucks=pucks), has_warnings


def parse_p11_crystals(proposal_path: Path) -> Tuple[List[P11Crystal], bool]:
    """Parses all paths below the proposal path, assuming there are crystals directly below that"""
    if not proposal_path.is_dir():
        raise Exception(f"proposal path {proposal_path} is not a directory!")
    raw_path = proposal_path / "raw"
    if not raw_path.is_dir():
        raise Exception(f"proposal raw data {raw_path} is not a directory!")
    result: List[P11Crystal] = []
    has_warnings = False
    for crystal_dir in raw_path.iterdir():
        if not crystal_dir.is_dir():
            continue

        crystal_info, crystal_has_warnings = parse_crystal(crystal_dir)

        has_warnings = has_warnings or crystal_has_warnings

        if crystal_info is not None:
            result.append(crystal_info)

    return result, has_warnings


def parse_p11_targets(proposal_path: Path) -> Tuple[List[P11Target], bool]:
    """
    Parses directories below the proposal path, assuming directly below that
    are directories for the different targets.
    """
    if not proposal_path.is_dir():
        raise Exception(f"proposal path {proposal_path} is not a directory!")
    raw_path = proposal_path / "raw"
    if not raw_path.is_dir():
        raise Exception(f"proposal raw data {raw_path} is not a directory!")
    result: List[P11Target] = []
    has_warnings = False
    for target_dir in raw_path.iterdir():
        if not target_dir.is_dir():
            continue

        target_info, target_has_warnings = parse_target(target_dir)

        has_warnings = has_warnings or target_has_warnings

        if target_info is not None:
            result.append(target_info)

    return result, has_warnings
