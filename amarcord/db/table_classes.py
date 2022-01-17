import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import List
from typing import Optional

from amarcord.db.attributi_map import AttributiMap
from amarcord.db.comment import DBComment
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.indexing_job_status import IndexingJobStatus


@dataclass(frozen=True)
class DBEvent:
    id: Optional[int]
    created: datetime.datetime
    level: EventLogLevel
    source: str
    text: str


@dataclass(frozen=True)
class DBFile:
    id: Optional[int]
    description: str
    type_: str
    file_name: str


@dataclass(frozen=True)
class DBSample:
    id: Optional[int]
    name: str
    attributi: AttributiMap
    files: List[DBFile]


@dataclass(frozen=True)
class DBRun:
    attributi: AttributiMap
    id: int
    sample_id: Optional[int]
    proposal_id: int
    modified: datetime.datetime
    comments: List[DBComment]


@dataclass(frozen=True)
class DBIndexingParameter:
    id: int
    project_file_first_discovery: datetime.datetime
    project_file_last_discovery: datetime.datetime
    project_file_path: Path
    project_file_content: Optional[str]
    geometry_file_content: Optional[str]
    project_file_hash: str


@dataclass(frozen=True)
class DBAugmentedIndexingParameter:
    indexing_parameter: DBIndexingParameter
    number_of_jobs: int


@dataclass(frozen=True)
class DBIndexingJob:
    id: int
    started: datetime.datetime
    stopped: Optional[datetime.datetime]
    output_directory: Path
    run_id: int
    indexing_parameter_id: int
    master_file: Path
    command_line: str
    status: IndexingJobStatus
    slurm_job_id: int
    result_file: Optional[Path]
    error_message: Optional[str]


@dataclass(frozen=True)
class DBRunWithIndexingResult:
    run_id: int
    indexing_jobs: List[DBIndexingJob]


@dataclass(frozen=True)
class DBIndexingRunData:
    run_id: int
    master_file: Path
    output_directory: Path
    command_line: str
    slurm_job_id: int
