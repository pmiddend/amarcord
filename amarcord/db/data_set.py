from dataclasses import dataclass

from amarcord.db.attributi_map import AttributiMap


@dataclass(frozen=True)
class DBDataSet:
    id: int
    experiment_type_id: int
    attributi: AttributiMap
