from dataclasses import dataclass

from amarcord.db.attributo_id import AttributoId
from amarcord.db.chemical_type import ChemicalType


@dataclass(frozen=True)
class AttributoNameAndRole:
    attributo_name: AttributoId
    chemical_role: ChemicalType
