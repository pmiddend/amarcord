from enum import Enum


class JobStatus(Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESSFUL = "successful"
    FAILED = "failed"

    def is_done(self) -> bool:
        return self in (JobStatus.SUCCESSFUL, JobStatus.FAILED)
