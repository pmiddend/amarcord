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
    failure_reason: Optional[str] = None
    output_directory: Optional[Path] = None
    tool_id: Optional[int] = None
    tool_inputs: Optional[JSONDict] = None
    metadata: Optional[JSONDict] = None
    started: Optional[datetime.datetime] = None
    stopped: Optional[datetime.datetime] = None
