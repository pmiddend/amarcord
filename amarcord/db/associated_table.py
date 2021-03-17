from enum import Enum


class AssociatedTable(Enum):
    RUN = "run"
    SAMPLE = "sample"

    def pretty_id(self) -> str:
        # pylint: disable=no-member
        return self.value.capitalize()
