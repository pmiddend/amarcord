from enum import Enum


class JobStatus(Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESSFUL = "successful"
    FAILED = "failed"
