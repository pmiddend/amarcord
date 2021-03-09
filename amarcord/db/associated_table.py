from enum import Enum


class AssociatedTable(Enum):
    RUN = "run"
    SAMPLE = "sample"

    def pretty_id(self) -> str:
        return self.value.capitalize()
