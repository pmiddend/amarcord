from dataclasses import dataclass


@dataclass(frozen=True, eq=True)
class DBMiniChemical:
    chemical_id: int
    chemical_name: str
