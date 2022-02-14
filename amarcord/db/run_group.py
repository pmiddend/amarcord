from dataclasses import dataclass
from typing import List, Dict

from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_value import AttributoValue


@dataclass
class RunGroup:
    run_ids: List[int]
    total_minutes: int
    attributi_values: Dict[AttributoId, AttributoValue]
