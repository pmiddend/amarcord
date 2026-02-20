from enum import StrEnum
from enum import unique


# str to make it JSON serializable
@unique
class AssociatedTable(StrEnum):
    RUN = "run"
    CHEMICAL = "chemical"
