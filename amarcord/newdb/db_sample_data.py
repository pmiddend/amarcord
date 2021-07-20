from dataclasses import dataclass
from typing import List

from amarcord.newdb.db_crystal import DBCrystal
from amarcord.newdb.db_puck import DBPuck


@dataclass(frozen=True)
class DBSampleData:
    pucks: List[DBPuck]
    crystals: List[DBCrystal]
