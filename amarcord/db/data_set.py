from dataclasses import dataclass

from amarcord.db.attributi_map import AttributiMap


@dataclass(frozen=True)
class DBDataSet:
    id: int
    experiment_type: str
    attributi: AttributiMap
