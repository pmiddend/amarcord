from dataclasses import dataclass

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoType
from amarcord.db.beamtime_id import BeamtimeId


@dataclass(frozen=True)
class DBAttributo:
    id: AttributoId
    beamtime_id: BeamtimeId
    name: str
    description: str
    group: str
    associated_table: AssociatedTable
    attributo_type: AttributoType

    def pretty_id(self) -> str:
        return self.description if self.description else self.name
