import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from amarcord.modules.json import JSONDict
from amarcord.newdb.db_tool import DBTool
from amarcord.workflows.job_status import JobStatus


@dataclass(frozen=True)
class DBReductionJob:
    id: int
    queued: datetime.datetime
    started: Optional[datetime.datetime]
    stopped: Optional[datetime.datetime]
    failure_reason: Optional[str]
    status: JobStatus
    output_directory: Optional[Path]
    run_id: int
    crystal_id: str
    data_raw_filename_pattern: Optional[str]
    metadata: JSONDict
    tool: DBTool
    tool_inputs: JSONDict
