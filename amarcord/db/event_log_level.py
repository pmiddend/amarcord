from enum import StrEnum
from enum import unique


# str to make it JSON serializable
@unique
class EventLogLevel(StrEnum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    USER = "user"
