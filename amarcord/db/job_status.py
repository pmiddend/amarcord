from enum import Enum


class DBJobStatus(Enum):
    RUNNING = "running"
    SUCCESS = "success"
    FAILURE = "failure"
