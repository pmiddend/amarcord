import datetime
from dataclasses import dataclass

from amarcord.modules.json import JSONDict
from amarcord.workflows.job_status import JobStatus


@dataclass(frozen=True)
class Job:
    status: JobStatus
    started: datetime.datetime
    metadata: JSONDict
