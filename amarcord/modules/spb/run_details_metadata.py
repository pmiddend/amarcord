import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

from PyQt5 import QtCore, QtGui, QtWidgets

from amarcord.modules.spb.colors import color_manual_run_property
from amarcord.modules.spb.queries import Run, SPBQueries
from amarcord.modules.spb.run_property import RunProperty
from amarcord.qt.properties import delegate_for_property_type

logger = logging.getLogger(__name__)


class _MetadataColumn(Enum):
    NAME = 0
    VALUE = 1


@dataclass(frozen=True)
class RunPropWithName:
    prop: RunProperty
    name: str


class _MetadataModel(QtCore.QAbstractTableModel):
    def __init__(
        self,
        db: SPBQueries,
        run: Run,
        sample_ids: List[int],
        available_tags: List[str],
        parent: Optional[QtCore.QObject] = None,
    ) -> None:
        super().__init__(parent)

        self._db = db
        self._headers = ["Field", "Value"]
        self._run = run
        self._sample_ids = sample_ids
        self._available_tags = available_tags
        with db.connect() as conn:
            self._props = [
                RunPropWithName(k, v)
                for k, v in db.run_property_names(conn).items()
                if k not in (db.tables.property_comments, db.tables.property_karabo)
            ]
            self._props.sort(key=lambda x: x.name)
            self._row_index_to_property: Dict[int, RunPropWithName] = {
                idx: v for idx, v in enumerate(self._props)
            }

    def delegate_for_row(self, row: int) -> QtWidgets.QAbstractItemDelegate:
        return delegate_for_property_type(
            self._db.tables.property_types[self._row_index_to_property[row].prop],
            sample_ids=self._sample_ids,
            available_tags=self._available_tags,
        )

    def rowCount(self, parent: QtCore.QModelIndex = QtCore.QModelIndex()) -> int:
        return len(self._props)

    def columnCount(self, parent: QtCore.QModelIndex = QtCore.QModelIndex()) -> int:
        return 2

    def flags(self, index: QtCore.QModelIndex) -> QtCore.Qt.ItemFlags:
        flags = super().flags(index)
        if index.column() != _MetadataColumn.VALUE.value:
            return flags
        # noinspection PyTypeChecker
        return flags | QtCore.Qt.ItemIsEditable  # type: ignore

    def setData(
        self, index: QtCore.QModelIndex, value: Any, role: int = QtCore.Qt.EditRole
    ) -> bool:
        assert index.column() == _MetadataColumn.VALUE.value
        if role != QtCore.Qt.EditRole:
            return False
        runprop = self._row_index_to_property[index.row()]
        with self._db.connect() as conn:
            self._db.update_run_property(
                conn,
                self._run.properties[self.db.tables.property_run_id],
                runprop.prop,
                value,
            )
            self._run.properties[runprop.prop] = value
            self.dataChanged.emit(
                index,
                index,
            )
            return True

    def headerData(
        self,
        section: int,
        orientation: QtCore.Qt.Orientation,
        role: int = QtCore.Qt.DisplayRole,
    ) -> QtCore.QVariant:
        if (
            orientation == QtCore.Qt.Vertical
            or role != QtCore.Qt.DisplayRole
            or section > len(self._headers)
        ):
            return QtCore.QVariant()
        return QtCore.QVariant(self._headers[section])

    def data(
        self,
        index: QtCore.QModelIndex,
        role: int = QtCore.Qt.DisplayRole,
    ) -> Any:
        runprop = self._row_index_to_property[index.row()]
        if role == QtCore.Qt.EditRole:
            return self._run.properties.get(runprop.prop, None)
        if role == QtCore.Qt.DisplayRole:
            return (
                runprop.name
                if index.column() == _MetadataColumn.NAME.value
                else self._db.tables.run_property_to_string(
                    runprop.prop, self._run.properties.get(runprop.prop, None)
                )
                if runprop.prop in self._run.properties
                else "N/A"
            )
        if (
            role == QtCore.Qt.BackgroundRole
            and runprop.prop in self._db.tables.manual_properties
        ):
            return QtGui.QBrush(color_manual_run_property)
        return None


class MetadataTable(QtWidgets.QTableView):
    def __init__(
        self,
        db: SPBQueries,
        parent: Optional[QtWidgets.QWidget] = None,
    ) -> None:
        super().__init__(parent)
        self._db = db
        self.setAlternatingRowColors(True)
        self.verticalHeader().hide()
        self.horizontalHeader().setStretchLastSection(True)
        self._run = None
        self.setEditTriggers(QtWidgets.QAbstractItemView.DoubleClicked)
        self._item_delegates: List[QtWidgets.QAbstractItemDelegate] = []

    def run_changed(self, run: Run) -> None:
        with self._db.connect() as conn:
            model = _MetadataModel(
                db=self._db,
                run=run,
                sample_ids=self._db.retrieve_sample_ids(conn),
                available_tags=self._db.retrieve_tags(conn),
            )
            self.setModel(model)
            self.resizeColumnToContents(0)
            for row in range(model.rowCount()):
                delegate = model.delegate_for_row(row)
                self.setItemDelegateForRow(row, delegate)
                self._item_delegates.append(delegate)
