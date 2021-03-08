import logging
from enum import Enum
from functools import partial
from typing import Any, Callable, Dict, List, Optional, Tuple

from PyQt5 import QtWidgets
from PyQt5.QtGui import QBrush
from PyQt5.QtWidgets import QHBoxLayout

from amarcord.modules.spb.colors import COLOR_MANUAL_ATTRIBUTO
from amarcord.db.db import (
    DBOggetto,
)
from amarcord.db.attributo_id import AttributoId
from amarcord.db.tables import DBTables
from amarcord.qt.declarative_table import Column, Data, DeclarativeTable, Row
from amarcord.db.attributi import DBAttributo, delegate_for_property_type

logger = logging.getLogger(__name__)


class _MetadataColumn(Enum):
    NAME = 0
    VALUE = 1


class MetadataTable(QtWidgets.QWidget):
    def __init__(self, property_change: Callable[[AttributoId, Any], None]) -> None:
        super().__init__(None)

        self._property_change = property_change
        self._columns = [
            Column(header_label="Name", editable=False),
            Column(header_label="Value", editable=True),
        ]
        layout = QHBoxLayout()
        self.setLayout(layout)
        self._table = DeclarativeTable(
            Data(
                rows=[],
                columns=self._columns,
                row_delegates={},
                column_delegates={},
            ),
            parent=self,
        )
        self._table.setAlternatingRowColors(True)
        self._table.verticalHeader().hide()
        self._table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self._table)
        self._oggetto: Optional[DBOggetto] = None
        self.metadata: Dict[AttributoId, DBAttributo] = {}

    def _property_changed(self, prop: AttributoId, new_value: Any) -> None:
        self._property_change(prop, new_value)

    def _attributo_to_string(self, metadata: DBAttributo, value: Any) -> str:
        suffix: str = getattr(metadata.rich_property_type, "suffix", None)
        value_str = ", ".join(value) if isinstance(value, list) else str(value)
        return value_str if suffix is None else f"{value_str} {suffix}"

    def data_changed(
        self,
        new_oggetto: DBOggetto,
        metadata: Dict[AttributoId, DBAttributo],
        tables: DBTables,
        sample_ids: List[int],
    ) -> None:

        attributi_changed = (
            self._oggetto is None or new_oggetto.attributi != self._oggetto.attributi
        )

        self._oggetto = new_oggetto

        metadata_changed = self.metadata != metadata

        self.metadata = metadata

        if not attributi_changed and not metadata_changed:
            return

        attributi: List[Tuple[AttributoId, DBAttributo]] = [
            (k, v)
            for k, v in metadata.items()
            if k not in (tables.property_comments, tables.property_karabo)
        ]
        attributi.sort(key=lambda x: x[1].name)

        self._table.set_data(
            Data(
                rows=[
                    Row(
                        display_roles=[
                            md.description if md.description else md.name,
                            self._attributo_to_string(
                                metadata[attributo], new_oggetto.attributi[attributo]
                            )
                            if attributo in new_oggetto.attributi
                            else "None",
                        ],
                        edit_roles=[None, new_oggetto.attributi.get(attributo, None)],
                        background_roles={
                            0: QBrush(COLOR_MANUAL_ATTRIBUTO),
                            1: QBrush(COLOR_MANUAL_ATTRIBUTO),
                        }
                        if attributo in new_oggetto.manual_attributi
                        else {},
                        change_callbacks=[
                            None,
                            partial(self._property_changed, attributo),
                        ],
                    )
                    for attributo, md in attributi
                ],
                columns=self._columns,
                row_delegates={
                    idx: delegate_for_property_type(
                        md.rich_property_type,
                        sample_ids,
                    )
                    for (idx, (property, md)) in enumerate(attributi)
                    if md.rich_property_type is not None
                },
                column_delegates={},
            )
        )
