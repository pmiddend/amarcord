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
    attributo_type_to_string,
    delegate_for_property_type,
)
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.raw_attributi_map import RawAttributiMap
from amarcord.db.rich_attributo_type import PropertyComments, RichAttributoType
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


def _is_editable(attributo_type: Optional[RichAttributoType]) -> bool:
    return attributo_type is not None and not isinstance(
        attributo_type, PropertyComments
    )


class AttributiTable(QtWidgets.QWidget):
    def __init__(self, attributo_change: Callable[[AttributoId, Any], None]) -> None:
        super().__init__(None)

        self._attributo_change = attributo_change
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
        self._raw_attributi: Optional[RawAttributiMap] = None
        self._attributi: Optional[AttributiMap] = None
        self.metadata: Dict[AttributoId, DBAttributo] = {}

    def _attributo_changed(self, prop: AttributoId, new_value: Any) -> None:
        self._attributo_change(prop, new_value)

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
                attributo_type_to_string(self.metadata[attributo.name]),
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
                partial(self._attributo_changed, attributo.name),
                None,
            ],
        )

    def data_changed(
        self,
        new_attributi: RawAttributiMap,
        metadata: Dict[AttributoId, DBAttributo],
        sample_ids: List[int],
    ) -> None:
        metadata_changed = self.metadata != metadata

        self.metadata = deepcopy(metadata)

        attributi_changed = (
            self._raw_attributi is None or new_attributi != self._raw_attributi
        )

        if not attributi_changed and not metadata_changed:
            return

        self._attributi = AttributiMap(metadata, new_attributi)

        display_attributi: List[DBAttributo] = sorted(
            [k for k in metadata.values() if _is_editable(k.rich_property_type)],
            key=lambda x: x.name,
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
                },
                column_delegates={},
            )
        )
