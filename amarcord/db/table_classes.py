import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import List
from typing import Optional

from amarcord.db.attributi_map import AttributiMap
from amarcord.db.event_log_level import EventLogLevel


@dataclass(frozen=True)
class DBEvent:
    id: int
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
    size_in_bytes: int


@dataclass(frozen=True)
class DBSample:
    id: int
    name: str
    attributi: AttributiMap
    files: List[DBFile]


@dataclass(frozen=True)
class DBRun:
    id: int
    attributi: AttributiMap
    files: List[DBFile]


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
class DBIndexingRunData:
    run_id: int
    master_file: Path
    output_directory: Path
    command_line: str
    slurm_job_id: int
