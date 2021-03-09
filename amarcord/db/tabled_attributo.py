from dataclasses import dataclass

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributi import DBAttributo


@dataclass(frozen=True)
class TabledAttributo:
    table: AssociatedTable
    attributo: DBAttributo

    def __repr__(self) -> str:
        return f"{self.table.value}." + (
            self.attributo.description
            if self.attributo.description
            else self.attributo.name
        )

    def __str__(self) -> str:
        return f"{self.table.value}." + (
            self.attributo.description
            if self.attributo.description
            else self.attributo.name
        )

    def pretty_id(self) -> str:
        return f"{self.table.pretty_id()}.{self.attributo.pretty_id()}"

    def technical_id(self) -> str:
        return f"{self.table.value}." + self.attributo.name
