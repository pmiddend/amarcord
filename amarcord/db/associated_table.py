from enum import Enum
from enum import unique


# str to make it JSON serializable
@unique
class AssociatedTable(str, Enum):
    RUN = "run"
    CHEMICAL = "chemical"
