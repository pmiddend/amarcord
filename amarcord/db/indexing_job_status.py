from enum import Enum


class IndexingJobStatus(Enum):
    RUNNING = "running"
    SUCCESS = "success"
    FAIL = "fail"
