from enum import Enum
from enum import unique


@unique
class IndexingJobStatus(Enum):
    RUNNING = "running"
    SUCCESS = "success"
    FAIL = "fail"
