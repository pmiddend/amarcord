from enum import Enum, unique


@unique
class IndexingJobStatus(Enum):
    RUNNING = "running"
    SUCCESS = "success"
    FAIL = "fail"
