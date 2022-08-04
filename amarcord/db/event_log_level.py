import logging
from enum import Enum
from enum import unique


@unique
class EventLogLevel(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"

    def to_python_log_level(self) -> int:
        return (
            logging.INFO
            if self == EventLogLevel.INFO
            else logging.WARNING
            if self == EventLogLevel.WARNING
            else logging.ERROR
            if self == EventLogLevel.ERROR
            else logging.INFO
        )
