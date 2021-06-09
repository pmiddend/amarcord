import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from typing import Union

from pint import UnitRegistry

from amarcord.amici.xds.parser import XDSCorrectLPFile
from amarcord.amici.xds.parser import XDSResultsFile
from amarcord.amici.xds.parser import parse_correctlp
from amarcord.amici.xds.parser import parse_resultsfile


@dataclass(frozen=True, eq=True)
class XDSFilesystem:
    correct_lp: XDSCorrectLPFile
    results_file: XDSResultsFile
    mtz_file: Optional[Path]
    analysis_time: datetime.datetime


@dataclass(frozen=True, eq=True)
class XDSFilesystemError:
    message: str
    log_file: Optional[Path]


def analyze_xds_filesystem(
    processed_path: Path, ureg: UnitRegistry
) -> Union[XDSFilesystem, XDSFilesystemError]:
    full_path = processed_path / "full"
    log_path = (
        full_path / "xdsapp.log" if (full_path / "xdsapp.log").is_file() else None
    )
    if not full_path.is_dir():
        return XDSFilesystemError(
            message=f'cannot find path "full" below {processed_path}', log_file=None
        )
    try:
        correct_lp = parse_correctlp(full_path / "CORRECT.LP", ureg)
    except Exception as e:
        return XDSFilesystemError(str(e), log_file=None)
    results_files = list(full_path.glob("results_*.txt"))
    if not results_files:
        return XDSFilesystemError(
            f"couldn't find any results file below {processed_path}",
            log_file=log_path,
        )
    if len(results_files) > 1:
        return XDSFilesystemError(
            f"found more than one results file: {results_files}", log_file=log_path
        )
    results_file = parse_resultsfile(results_files[0])

    mtz_files = list(full_path.glob("*_F.mtz"))
    # Sort by mtime, take the first (so the latest) one below
    mtz_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)

    analysis_time = datetime.datetime.fromtimestamp(
        processed_path.stat().st_mtime, tz=datetime.timezone.utc
    )

    return XDSFilesystem(
        correct_lp=correct_lp,
        results_file=results_file,
        analysis_time=analysis_time,
        mtz_file=next(iter(mtz_files), None),
    )
