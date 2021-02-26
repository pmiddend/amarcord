from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
import logging

from PyQt5.QtCore import (
    QAbstractTableModel,
    QModelIndex,
    QObject,
    QVariant,
    Qt,
    pyqtSignal,
)
from PyQt5.QtWidgets import (
    QAbstractItemDelegate,
    QAbstractItemView,
    QStyledItemDelegate,
    QTableView,
    QWidget,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, eq=True)
class Row:
    display_roles: List[str]
    edit_roles: List[Optional[Any]]
    change_callbacks: List[Optional[Callable[[Any], None]]] = field(
        hash=False, default_factory=lambda: [], compare=False
    )


@dataclass(frozen=True, eq=True)
class Column:
    header_label: str
    editable: bool


@dataclass(frozen=True, eq=True)
class Data:
    rows: List[Row]
    columns: List[Column]
    row_delegates: Dict[int, QAbstractItemDelegate] = field(hash=False, compare=False)
    column_delegates: Dict[int, QAbstractItemDelegate] = field(
        hash=False, compare=False
    )


class DeclarativeTableModel(QAbstractTableModel):
    cell_changed = pyqtSignal(QModelIndex, QVariant, int)

    def __init__(self, data: Data, parent: Optional[QObject] = None) -> None:
        super().__init__(parent)

        self._data = data

    def rowCount(self, _parent: QModelIndex = QModelIndex()) -> int:
        return len(self._data.rows)

    def columnCount(self, _parent: QModelIndex = QModelIndex()) -> int:
        return len(self._data.columns)

    def flags(self, index: QModelIndex) -> Qt.ItemFlags:
        flags = super().flags(index)
        if self._data.columns[index.column()].editable:
            # noinspection PyTypeChecker
            return flags | Qt.ItemIsEditable  # type: ignore
        return flags

    def headerData(
        self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole
    ) -> Any:
        if orientation == Qt.Vertical or role != Qt.DisplayRole:
            return None
        return self._data.columns[section].header_label

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole) -> Any:
        # logger.info("model.data(%s, %s)", index.row(), index.column())
        if role == Qt.DisplayRole:
            return self._data.rows[index.row()].display_roles[index.column()]
        if role == Qt.EditRole:
            return self._data.rows[index.row()].edit_roles[index.column()]
        return None

    def set_data(self, data: Data) -> None:
        # old_data = self._data

        # Here we can, later on, compare old/new and emit dataChanged signals
        # (or layout change if everything changed)

        # noinspection PyUnresolvedReferences
        self.modelAboutToBeReset.emit()  # type: ignore
        self._data = data
        # noinspection PyUnresolvedReferences
        self.modelReset.emit()  # type: ignore

    def setData(self, index: QModelIndex, value: Any, role: int = Qt.EditRole) -> bool:
        if not index.isValid():
            return False
        # self.dataChanged.emit(QModelIndex(), QModelIndex())
        self.cell_changed.emit(index, value, role)
        # self._data.rows[index.row()].change_callbacks[index.column()](value)
        return False


class DeclarativeTable(QTableView):
    def __init__(self, data: Data, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)

        self._data = data
        model = DeclarativeTableModel(data)
        self.setModel(model)
        model.cell_changed.connect(self._cell_changed)

        for row_idx, delegate in data.row_delegates.items():
            self.setItemDelegateForRow(row_idx, delegate)

        for row_idx, delegate in data.column_delegates.items():
            self.setItemDelegateForColumn(row_idx, delegate)

        self.setEditTriggers(QAbstractItemView.DoubleClicked)

    def _cell_changed(self, index: QModelIndex, new_value: Any, _role: int) -> None:
        cb = self._data.rows[index.row()].change_callbacks[index.column()]
        if cb is not None:
            cb(new_value)

    def set_data(self, data: Data) -> None:
        for row_idx, delegate in self._data.row_delegates.items():
            # noinspection PyTypeChecker
            self.setItemDelegateForRow(row_idx, QStyledItemDelegate())  # type: ignore

        for row_idx, delegate in self._data.column_delegates.items():
            # noinspection PyTypeChecker
            self.setItemDelegateForColumn(row_idx, QStyledItemDelegate())  # type: ignore

        self.model().set_data(data)
        self._data = data

        for row_idx, delegate in data.row_delegates.items():
            self.setItemDelegateForRow(row_idx, delegate)

        for row_idx, delegate in data.column_delegates.items():
            self.setItemDelegateForColumn(row_idx, delegate)
