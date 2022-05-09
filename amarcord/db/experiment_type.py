from dataclasses import dataclass
from typing import List

from amarcord.db.attributo_id import AttributoId


@dataclass(frozen=True)
class DBExperimentType:
    name: str
    attributo_names: List[AttributoId]
