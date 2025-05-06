from dataclasses import dataclass
from pathlib import Path
from typing import Final

from amarcord.web.json_models import JsonAlignDetectorGroup


@dataclass(frozen=True, eq=True)
class IndexingResultSummary:
    hit_rate: float
    indexing_rate: float
    indexed_frames: int
    align_detector_groups: list[JsonAlignDetectorGroup]


empty_indexing_fom: Final = IndexingResultSummary(
    hit_rate=0.0, indexing_rate=0.0, indexed_frames=0, align_detector_groups=[]
)


@dataclass(frozen=True, eq=True)
class DBIndexingResultRunning:
    stream_file: Path
    job_id: int
    fom: IndexingResultSummary


@dataclass(frozen=True, eq=True)
class DBIndexingResultDone:
    stream_file: Path
    job_error: None | str
    fom: IndexingResultSummary
