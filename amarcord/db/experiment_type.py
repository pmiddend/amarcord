from dataclasses import dataclass

from amarcord.db.attributo_name_and_role import AttributoIdAndRole


@dataclass(frozen=True)
class DBExperimentType:
    id: int
    name: str
    attributi: list[AttributoIdAndRole]
