from enum import Enum


class AssociatedTable(Enum):
    RUN = "run"
    SAMPLE = "sample"

    def pretty_id(self) -> str:
        # pylint: disable=no-member
        return self.value.capitalize()

    def sort_key(self) -> int:
        return (
            0
            if self == AssociatedTable.RUN
            else 1
            if self == AssociatedTable.SAMPLE
            else 2
        )
