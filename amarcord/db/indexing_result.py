from dataclasses import dataclass
from pathlib import Path
from typing import Final


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
