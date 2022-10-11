import datetime
from dataclasses import dataclass
from typing import TypeAlias

from amarcord.amici.crystfel.util import CrystFELCellFile
from amarcord.db.indexing_result import DBIndexingResultOutput
from amarcord.db.merge_negative_handling import MergeNegativeHandling
from amarcord.db.merge_result import MergeResult


@dataclass(frozen=True, eq=True)
class DBMergeRuntimeStatusRunning:
    job_id: int
    started: datetime.datetime
    recent_log: str


@dataclass(frozen=True, eq=True)
class DBMergeRuntimeStatusError:
    error: str
    started: datetime.datetime
    stopped: datetime.datetime
    recent_log: str


@dataclass(frozen=True, eq=True)
class DBMergeRuntimeStatusDone:
    started: datetime.datetime
    stopped: datetime.datetime
    result: MergeResult
    recent_log: str


DBMergeRuntimeStatus: TypeAlias = (
    None
    | DBMergeRuntimeStatusRunning
    | DBMergeRuntimeStatusError
    | DBMergeRuntimeStatusDone
)


@dataclass(frozen=True, eq=True)
class DBMergeResultInput:
    created: datetime.datetime
    indexing_results: list[DBIndexingResultOutput]
    point_group: str
    cell_description: CrystFELCellFile
    partialator_additional: str
    negative_handling: None | MergeNegativeHandling
    runtime_status: DBMergeRuntimeStatus


@dataclass(frozen=True, eq=True)
class DBMergeResultOutput:
    id: int
    created: datetime.datetime
    indexing_results: list[DBIndexingResultOutput]
    point_group: str
    cell_description: CrystFELCellFile
    partialator_additional: str
    negative_handling: None | MergeNegativeHandling
    runtime_status: DBMergeRuntimeStatus
