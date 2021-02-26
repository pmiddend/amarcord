import logging
from dataclasses import dataclass
from enum import Enum
from functools import partial
from typing import Any, Callable, Dict, List, Optional, Tuple

from PyQt5 import QtWidgets
from PyQt5.QtWidgets import QHBoxLayout

from amarcord.modules.spb.tables import Tables
from amarcord.modules.spb.queries import (
    Run,
    RunPropertyMetadata,
)
from amarcord.modules.spb.run_property import RunProperty
from amarcord.qt.declarative_table import Column, Data, DeclarativeTable, Row
from amarcord.qt.properties import RichPropertyType, delegate_for_property_type

logger = logging.getLogger(__name__)


class _MetadataColumn(Enum):
    NAME = 0
    VALUE = 1


@dataclass(frozen=True)
class AugmentedRunProperty:
    prop: RunProperty
    name: str
    rich_prop_type: RichPropertyType


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
        self._run: Optional[Run] = None

    def _property_changed(self, prop: RunProperty, new_value: Any) -> None:
        self._property_change(prop, new_value)

    def data_changed(
        self,
        run: Run,
        metadata: Dict[RunProperty, RunPropertyMetadata],
        tables: Tables,
        sample_ids: List[int],
        tags: List[str],
    ) -> None:

        self._run = run

        run_properties: List[Tuple[RunProperty, RunPropertyMetadata]] = [
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
                            md.name,
                            tables.run_property_to_string(
                                property, run.properties[property]
                            )
                            if property in run.properties
                            else "None",
                        ],
                        edit_roles=[None, run.properties.get(property, None)],
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
                        md.rich_prop_type,
                        sample_ids,
                        tags,
                    )
                    for (idx, (property, md)) in enumerate(run_properties)
                    if md.rich_prop_type is not None
                },
                column_delegates={},
            )
        )
