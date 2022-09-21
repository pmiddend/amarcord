import datetime
from dataclasses import dataclass
from typing import NewType

from amarcord.amici.workload_manager.job_status import JobStatus
from amarcord.json_types import JSONDict

JobMetadata = NewType("JobMetadata", JSONDict)


@dataclass(frozen=True)
class Job:
    status: JobStatus
    started: datetime.datetime
    id: int
    metadata: JobMetadata
