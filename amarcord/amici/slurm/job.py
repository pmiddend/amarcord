import datetime
from dataclasses import dataclass

from amarcord.json import JSONDict
from amarcord.amici.slurm.job_status import JobStatus


@dataclass(frozen=True)
class Job:
    status: JobStatus
    started: datetime.datetime
    metadata: JSONDict
