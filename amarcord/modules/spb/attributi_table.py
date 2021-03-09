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
    PropertyChoice,
    PropertyComments,
    PropertyDateTime,
    PropertyDouble,
    PropertyInt,
    PropertySample,
    PropertyString,
    PropertyTags,
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


def _attributo_value_to_string(metadata: DBAttributo, value: Any) -> str:
    return ", ".join(value) if isinstance(value, list) else str(value)
    # suffix: str = getattr(metadata.rich_property_type, "suffix", None)
    # value_str = ", ".join(value) if isinstance(value, list) else str(value)
    # return value_str if suffix is None else f"{value_str} {suffix}"


def _attributo_type_to_string(attributo: DBAttributo) -> str:
    pt = attributo.rich_property_type
    if isinstance(pt, PropertyInt):
        return "integer"
    if isinstance(pt, PropertyChoice):
        return "choice"
    if isinstance(pt, PropertyDouble):
        if attributo.suffix:
            return (
                f"{attributo.suffix} (range {pt.range})"
                if pt.range is not None
                else attributo.suffix
            )
        return f"number in {pt.range}" if pt is not None else "number"
    if isinstance(pt, PropertyTags):
        return "tags"
    if isinstance(pt, PropertySample):
        return "Sample ID"
    if isinstance(pt, PropertyString):
        return "text"
    if isinstance(pt, PropertyComments):
        return "comments"
    if isinstance(pt, PropertyDateTime):
        return "date and time"
    raise Exception(f"invalid property type {type(pt)}")


class AttributiTable(QtWidgets.QWidget):
    def __init__(self, property_change: Callable[[AttributoId, Any], None]) -> None:
        super().__init__(None)

        self._property_change = property_change
        self._columns = [
            Column(header_label="Name", editable=False),
            Column(header_label="Value", editable=True),
            Column(header_label="Type", editable=False),
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
        layout.addWidget(self._table)
        self._attributi: Optional[AttributiMap] = None
        self.metadata: Dict[AttributoId, DBAttributo] = {}

    def _property_changed(self, prop: AttributoId, new_value: Any) -> None:
        self._property_change(prop, new_value)

    def _build_row(self, attributo: DBAttributo) -> Row:
        selected = (
            self._attributi.select(attributo.name)
            if self._attributi is not None
            else None
        )
        return Row(
            display_roles=[
                attributo.description if attributo.description else attributo.name,
                _attributo_value_to_string(
                    self.metadata[attributo.name], selected.value
                )
                if selected is not None
                else "",
                _attributo_type_to_string(self.metadata[attributo.name]),
            ],
            edit_roles=[None, selected.value if selected is not None else None, None],
            background_roles={
                0: QBrush(COLOR_MANUAL_ATTRIBUTO),
                1: QBrush(COLOR_MANUAL_ATTRIBUTO),
                2: QBrush(COLOR_MANUAL_ATTRIBUTO),
            }
            if selected is not None and selected.source == MANUAL_SOURCE_NAME
            else {},
            change_callbacks=[
                None,
                partial(self._property_changed, attributo.name),
                None,
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
                    and not isinstance(md.rich_property_type, PropertyComments)
                },
                column_delegates={},
            )
        )
