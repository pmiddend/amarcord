from enum import Enum
from enum import unique


@unique
class AssociatedTable(Enum):
    RUN = "run"
    CHEMICAL = "chemical"

    def pretty_id(self) -> str:
        # pylint: disable=no-member
        return self.value.capitalize()

    def sort_key(self) -> int:
        return (
            0
            if self == AssociatedTable.RUN
            else 1
            if self == AssociatedTable.CHEMICAL
            else 2
        )
