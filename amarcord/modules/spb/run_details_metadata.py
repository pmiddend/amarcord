import logging
from enum import Enum
from functools import partial
from typing import Any, Callable, Dict, List, Optional, Tuple

from PyQt5 import QtWidgets
from PyQt5.QtGui import QBrush
from PyQt5.QtWidgets import QHBoxLayout

from amarcord.modules.spb.colors import COLOR_MANUAL_RUN_PROPERTY
from amarcord.modules.spb.db import (
    DBCustomProperty,
    DBRun,
)
from amarcord.modules.spb.run_property import RunProperty
from amarcord.modules.spb.db_tables import DBTables
from amarcord.qt.declarative_table import Column, Data, DeclarativeTable, Row
from amarcord.modules.properties import delegate_for_property_type

logger = logging.getLogger(__name__)


class _MetadataColumn(Enum):
    NAME = 0
    VALUE = 1


class MetadataTable(QtWidgets.QWidget):
    def __init__(self, property_change: Callable[[RunProperty, Any], None]) -> None:
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
        self._run: Optional[DBRun] = None
        self._metadata: Dict[RunProperty, DBCustomProperty] = {}

    def _property_changed(self, prop: RunProperty, new_value: Any) -> None:
        self._property_change(prop, new_value)

    def _run_property_to_string(self, metadata: DBCustomProperty, value: Any) -> str:
        suffix: str = getattr(metadata.rich_property_type, "suffix", None)
        value_str = ", ".join(value) if isinstance(value, list) else str(value)
        return value_str if suffix is None else f"{value_str} {suffix}"

    def data_changed(
        self,
        run: DBRun,
        metadata: Dict[RunProperty, DBCustomProperty],
        tables: DBTables,
        sample_ids: List[int],
    ) -> None:

        run_properties_changed = (
            self._run is None or run.properties != self._run.properties
        )

        self._run = run

        metadata_changed = self._metadata != metadata

        self._metadata = metadata

        if not run_properties_changed and not metadata_changed:
            return

        run_properties: List[Tuple[RunProperty, DBCustomProperty]] = [
            (k, v)
            for k, v in metadata.items()
            if k not in (tables.property_comments, tables.property_karabo)
        ]
        run_properties.sort(key=lambda x: x[1].name)

        self._table.set_data(
            Data(
                rows=[
                    Row(
                        display_roles=[
                            md.description if md.description else md.name,
                            self._run_property_to_string(
                                metadata[property], run.properties[property]
                            )
                            if property in run.properties
                            else "None",
                        ],
                        edit_roles=[None, run.properties.get(property, None)],
                        background_roles={
                            0: QBrush(COLOR_MANUAL_RUN_PROPERTY),
                            1: QBrush(COLOR_MANUAL_RUN_PROPERTY),
                        }
                        if property in run.manual_properties
                        else {},
                        change_callbacks=[
                            None,
                            partial(self._property_changed, property),
                        ],
                    )
                    for property, md in run_properties
                ],
                columns=self._columns,
                row_delegates={
                    idx: delegate_for_property_type(
                        md.rich_property_type,
                        sample_ids,
                    )
                    for (idx, (property, md)) in enumerate(run_properties)
                    if md.rich_property_type is not None
                },
                column_delegates={},
            )
        )
