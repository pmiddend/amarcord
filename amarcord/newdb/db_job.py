import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from amarcord.modules.json import JSONDict
from amarcord.workflows.job_status import JobStatus


@dataclass(frozen=True)
class DBJob:
    id: Optional[int]
    queued: datetime.datetime
    status: JobStatus
    tool_id: int
    tool_inputs: JSONDict
    failure_reason: Optional[str] = None
    comment: Optional[str] = None
    output_directory: Optional[Path] = None
    metadata: Optional[JSONDict] = None
    started: Optional[datetime.datetime] = None
    stopped: Optional[datetime.datetime] = None
