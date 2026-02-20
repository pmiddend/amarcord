from enum import StrEnum


class DBJobStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    DONE = "done"
