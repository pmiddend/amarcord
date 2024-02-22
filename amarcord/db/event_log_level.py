from enum import Enum
from enum import unique


# str to make it JSON serializable
@unique
class EventLogLevel(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    USER = "user"
