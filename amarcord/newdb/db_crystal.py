import datetime
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class DBCrystal:
    crystal_id: str
    created: Optional[datetime.datetime] = None
    puck_id: Optional[str] = None
    puck_position: Optional[int] = None
