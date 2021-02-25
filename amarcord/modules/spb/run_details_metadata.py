import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from PyQt5 import QtCore, QtGui, QtWidgets

from amarcord.modules.spb.colors import color_manual_run_property
from amarcord.modules.spb.queries import (
    Connection,
    Run,
    RunPropertyMetadata,
    SPBQueries,
)
from amarcord.modules.spb.run_property import RunProperty
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
                (k, v)
                for k, v in db.run_property_metadata(conn).items()
                if k not in (db.tables.property_comments, db.tables.property_karabo)
            ]
            self._props.sort(key=lambda x: x[1].name)
            self._row_index_to_property: Dict[
                int, Tuple[RunProperty, RunPropertyMetadata]
            ] = {idx: v for idx, v in enumerate(self._props)}

    def delegate_for_row(self, row: int) -> Optional[QtWidgets.QAbstractItemDelegate]:
        prop_type = self._row_index_to_property[row][1].rich_prop_type
        if prop_type is None:
            return None
        return delegate_for_property_type(
            prop_type,
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
        runprop, _md = self._row_index_to_property[index.row()]
        with self._db.connect() as conn:
            self._db.update_run_property(
                conn,
                self._run.properties[self._db.tables.property_run_id],
                runprop,
                value,
            )
            self._run.properties[runprop] = value
            logger.info("Emitting data changed")
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
        runprop, metadata = self._row_index_to_property[index.row()]
        if role == QtCore.Qt.EditRole:
            return self._run.properties.get(runprop, None)
        if role == QtCore.Qt.DisplayRole:
            return (
                metadata.name
                if index.column() == _MetadataColumn.NAME.value
                else self._db.tables.run_property_to_string(
                    runprop, self._run.properties.get(runprop, None)
                )
                if runprop in self._run.properties
                else "N/A"
            )
        if (
            role == QtCore.Qt.BackgroundRole
            and runprop in self._db.tables.manual_properties
        ):
            return QtGui.QBrush(color_manual_run_property)
        return None


class MetadataTable(QtWidgets.QTableView):
    data_changed = QtCore.pyqtSignal()

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
        self._run: Optional[Run] = None
        self.setEditTriggers(QtWidgets.QAbstractItemView.DoubleClicked)
        self._item_delegates: List[QtWidgets.QAbstractItemDelegate] = []

    def _run_changed(self, conn: Connection, run: Run) -> None:
        self._run = run
        model = _MetadataModel(
            db=self._db,
            run=run,
            sample_ids=self._db.retrieve_sample_ids(conn),
            available_tags=self._db.retrieve_tags(conn),
        )
        self.setModel(model)
        model.dataChanged.connect(self._data_changed)
        self.resizeColumnToContents(0)
        for row in range(model.rowCount()):
            delegate = model.delegate_for_row(row)
            if delegate is not None:
                self.setItemDelegateForRow(row, delegate)
                self._item_delegates.append(delegate)

    def _data_changed(self) -> None:
        self.data_changed.emit()

    def custom_metadata_changed(self, conn: Connection) -> None:
        assert self._run is not None, "Metadata changed, but have no run"
        self._run_changed(conn, self._run)

    def run_changed(self, run: Run) -> None:
        with self._db.connect() as conn:
            self._run_changed(conn, run)
