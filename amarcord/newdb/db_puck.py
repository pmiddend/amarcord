from dataclasses import dataclass
from typing import Optional

from amarcord.newdb.puck_type import PuckType


@dataclass(frozen=True)
class DBPuck:
    id: Optional[str]
    puck_type: PuckType
    owner: str
