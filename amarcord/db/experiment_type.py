from dataclasses import dataclass

from amarcord.db.attributo_id import AttributoId


@dataclass(frozen=True)
class DBExperimentType:
    id: int
    name: str
    attributi_names: list[AttributoId]
