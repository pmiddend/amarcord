import logging
from copy import deepcopy
from enum import Enum
from functools import partial
from typing import Any, Callable, Dict, List, Optional

from PyQt5 import QtWidgets
from PyQt5.QtGui import QBrush
from PyQt5.QtWidgets import QHBoxLayout

from amarcord.db.attributi import (
    AttributiMap,
    DBAttributo,
    delegate_for_property_type,
)
from amarcord.db.attributo_id import AttributoId
from amarcord.db.constants import MANUAL_SOURCE_NAME
from amarcord.modules.spb.colors import COLOR_MANUAL_ATTRIBUTO
from amarcord.qt.declarative_table import Column, Data, DeclarativeTable, Row

logger = logging.getLogger(__name__)


class _MetadataColumn(Enum):
    NAME = 0
    VALUE = 1


def _attributo_to_string(metadata: DBAttributo, value: Any) -> str:
    suffix: str = getattr(metadata.rich_property_type, "suffix", None)
    value_str = ", ".join(value) if isinstance(value, list) else str(value)
    return value_str if suffix is None else f"{value_str} {suffix}"


class AttributiTable(QtWidgets.QWidget):
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
        self._attributi: Optional[AttributiMap] = None
        self.metadata: Dict[AttributoId, DBAttributo] = {}

    def _property_changed(self, prop: AttributoId, new_value: Any) -> None:
        self._property_change(prop, new_value)

    def _display_role_for_attributo(self, attributo_id: AttributoId) -> str:
        selected = self._select_attributo(attributo_id)
        return (
            self._attributo_to_string(self.metadata[attributo_id], selected.value)
            if selected is not None
            else ""
        )

    def _build_row(self, attributo: DBAttributo) -> Row:
        selected = (
            self._attributi.select(attributo.name)
            if self._attributi is not None
            else None
        )
        return Row(
            display_roles=[
                attributo.description if attributo.description else attributo.name,
                _attributo_to_string(self.metadata[attributo.name], selected.value)
                if selected is not None
                else "",
            ],
            edit_roles=[None, selected.value if selected is not None else None],
            background_roles={
                0: QBrush(COLOR_MANUAL_ATTRIBUTO),
                1: QBrush(COLOR_MANUAL_ATTRIBUTO),
            }
            if selected is not None and selected.source == MANUAL_SOURCE_NAME
            else {},
            change_callbacks=[
                None,
                partial(self._property_changed, attributo.name),
            ],
        )

    def data_changed(
        self,
        new_attributi: AttributiMap,
        metadata: Dict[AttributoId, DBAttributo],
        sample_ids: List[int],
    ) -> None:

        attributi_changed = self._attributi is None or new_attributi != self._attributi

        self._attributi = deepcopy(new_attributi)

        metadata_changed = self.metadata != metadata

        self.metadata = deepcopy(metadata)

        if not attributi_changed and not metadata_changed:
            return

        display_attributi: List[DBAttributo] = sorted(
            metadata.values(), key=lambda x: x.name
        )

        self._table.set_data(
            Data(
                rows=[self._build_row(attributo) for attributo in display_attributi],
                columns=self._columns,
                row_delegates={
                    idx: delegate_for_property_type(
                        md.rich_property_type,
                        sample_ids,
                    )
                    for (idx, md) in enumerate(display_attributi)
                    if md.rich_property_type is not None
                },
                column_delegates={},
            )
        )
