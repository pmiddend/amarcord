import datetime
from dataclasses import dataclass
from typing import NewType

from amarcord.json_types import JSONDict
from amarcord.amici.slurm.job_status import JobStatus

JobMetadata = NewType("JobMetadata", JSONDict)


@dataclass(frozen=True)
class Job:
    status: JobStatus
    started: datetime.datetime
    metadata: JobMetadata
