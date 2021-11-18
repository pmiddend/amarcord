import datetime
from dataclasses import dataclass

from amarcord.modules.p11.job_status import JobStatus


@dataclass(frozen=True)
class Job:
    status: JobStatus
    started: datetime.datetime
    job_id: int
