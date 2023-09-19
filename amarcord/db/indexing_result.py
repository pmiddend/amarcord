import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Final
from typing import TypeAlias

from amarcord.amici.crystfel.util import CrystFELCellFile


@dataclass(frozen=True, eq=True)
class DBIndexingFOM:
    hit_rate: float
    indexing_rate: float
    indexed_frames: int
    detector_shift_x_mm: None | float
    detector_shift_y_mm: None | float


empty_indexing_fom: Final = DBIndexingFOM(
    hit_rate=0.0,
    indexing_rate=0.0,
    indexed_frames=0,
    detector_shift_x_mm=None,
    detector_shift_y_mm=None,
)


@dataclass(frozen=True, eq=True)
class DBIndexingResultRunning:
    stream_file: Path
    job_id: int
    fom: DBIndexingFOM


@dataclass(frozen=True, eq=True)
class DBIndexingResultDone:
    stream_file: Path
    job_error: None | str
    fom: DBIndexingFOM


# The state of the indexing result can be "queued", "running" or "done". There used to be an enum for that here, plus
# some variables that could all be "None". But that lead to some really weird code, where we check the enum and then
# assert some variables to be not None. This solution is a bit more complicated, but safer to use, and it plays nice
# with Python 3.10's match statement
DBIndexingResultRuntimeStatus: TypeAlias = (
    None | DBIndexingResultRunning | DBIndexingResultDone
)


@dataclass(frozen=True, eq=True)
class DBIndexingResultInput:
    created: datetime.datetime
    run_id: int
    frames: None | int
    hits: None | int
    not_indexed_frames: None | int
    cell_description: None | CrystFELCellFile
    point_group: None | str
    chemical_id: int
    runtime_status: DBIndexingResultRuntimeStatus


@dataclass(frozen=True, eq=True)
class DBIndexingResultOutput:
    id: int
    created: datetime.datetime
    run_id: int
    frames: None | int
    hits: None | int
    not_indexed_frames: None | int
    cell_description: None | CrystFELCellFile
    point_group: None | str
    chemical_id: int
    runtime_status: DBIndexingResultRuntimeStatus


@dataclass(frozen=True, eq=True)
class DBIndexingResultStatistic:
    indexing_result_id: int
    time: datetime.datetime
    frames: int
    hits: int
    indexed_frames: int
    indexed_crystals: int
