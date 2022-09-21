from enum import Enum


class DBJobStatus(Enum):
    QUEUED = "queued"
    RUNNING = "running"
    DONE = "done"
