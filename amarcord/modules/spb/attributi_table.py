import logging
from copy import deepcopy
from enum import Enum
from functools import partial
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional

from PyQt5 import QtWidgets
from PyQt5.QtCore import QPoint
from PyQt5.QtGui import QBrush
from PyQt5.QtWidgets import QHBoxLayout
from PyQt5.QtWidgets import QMenu

from amarcord.db.attributi import attributo_type_to_string
from amarcord.db.attributi import pretty_print_attributo
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoType
from amarcord.db.attributo_type import AttributoTypeComments
from amarcord.db.attributo_value import AttributoValue
from amarcord.db.constants import MANUAL_SOURCE_NAME
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.raw_attributi_map import RawAttributiMap
from amarcord.db.raw_attributi_map import Source
from amarcord.db.table_delegates import delegate_for_attributo_type
from amarcord.modules.spb.colors import COLOR_MANUAL_ATTRIBUTO
from amarcord.qt.declarative_table import Column
from amarcord.qt.declarative_table import Data
from amarcord.qt.declarative_table import DeclarativeTable
from amarcord.qt.declarative_table import Row

logger = logging.getLogger(__name__)


class _MetadataColumn(Enum):
    NAME = 0
    VALUE = 1


def _is_editable(attributo_type: Optional[AttributoType]) -> bool:
    return attributo_type is not None and not isinstance(
        attributo_type, AttributoTypeComments
    )


class AttributiTable(QtWidgets.QWidget):
    def __init__(
        self,
        raw_attributi: RawAttributiMap,
        metadata: Dict[AttributoId, DBAttributo],
        sample_ids: List[int],
        attributo_change: Callable[[AttributoId, Any], None],
        remove_manual_attributo: Callable[[AttributoId], None],
    ) -> None:
        super().__init__(None)

        self._remove_manual_attributo = remove_manual_attributo
        self._attributo_change = attributo_change
        self._columns = [
            Column(header_label="Name", editable=False),
            Column(header_label="Value", editable=True, stretch=True),
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
        self._sample_ids = sample_ids
        self.metadata = metadata
        self.attributi = AttributiMap(metadata, raw_attributi)

        self._update_table()

    def _attributo_changed(self, prop: AttributoId, new_value: Any) -> None:
        self._attributo_change(prop, new_value)

    def _build_row(self, attributo: DBAttributo) -> Row:
        selected = (
            self.attributi.select(attributo.name)
            if self.attributi is not None
            else None
        )
        return Row(
            display_roles=[
                attributo.description if attributo.description else attributo.name,
                pretty_print_attributo(self.metadata[attributo.name], selected.value)
                if selected is not None
                else "",
                attributo_type_to_string(self.metadata[attributo.name].attributo_type),
            ],
            right_click_menu=partial(self._right_click_menu, attributo.name),
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

    def _right_click_menu(self, attributo_id: AttributoId, p: QPoint) -> None:
        per_source: Dict[Source, AttributoValue] = self.attributi.values_per_source(
            attributo_id
        )
        menu = QMenu(self)
        deleteAction = None
        if MANUAL_SOURCE_NAME in per_source:
            deleteAction = menu.addAction(
                "Delete manual value",
            )
        exploreMenu = menu.addMenu(
            "Values per source",
        )
        for source, value in per_source.items():
            exploreMenu.addAction(f"{source}: {value}")
        action = menu.exec_(p)
        if action == deleteAction:
            self._remove_manual_attributo(attributo_id)

    def _update_table(self) -> None:
        display_attributi: List[DBAttributo] = sorted(
            [k for k in self.metadata.values() if _is_editable(k.attributo_type)],
            key=lambda x: x.description if x.description else x.name,
        )

        self._table.set_data(
            Data(
                rows=[self._build_row(attributo) for attributo in display_attributi],
                columns=self._columns,
                row_delegates={
                    idx: delegate_for_attributo_type(
                        md.attributo_type,
                        self._sample_ids,
                    )
                    for (idx, md) in enumerate(display_attributi)
                },
                column_delegates={},
            )
        )

    def data_changed(
        self,
        new_attributi: RawAttributiMap,
        metadata: Dict[AttributoId, DBAttributo],
        sample_ids: List[int],
    ) -> None:
        metadata_changed = self.metadata != metadata

        attributi_changed = new_attributi != self.attributi.to_raw()

        sample_ids_changed = sample_ids != self._sample_ids

        if not attributi_changed and not metadata_changed and not sample_ids_changed:
            return

        self.metadata = deepcopy(metadata)
        self.attributi = AttributiMap(metadata, new_attributi)
        self._sample_ids = sample_ids
        self._update_table()

    def set_single_manual(self, attributo: AttributoId, value: AttributoValue) -> None:
        self.attributi.set_single_manual(attributo, value)
        self._update_table()

    def remove_manual(self, attributo: AttributoId) -> None:
        self.attributi.remove_manual(attributo)
        self._update_table()
