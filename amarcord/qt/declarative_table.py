from dataclasses import dataclass, field
from functools import partial
from typing import Any, Callable, Dict, List, Optional
import logging

from PyQt5.QtCore import (
    QAbstractTableModel,
    QModelIndex,
    QObject,
    QPoint,
    QVariant,
    Qt,
    pyqtSignal,
)
from PyQt5.QtGui import QBrush
from PyQt5.QtWidgets import (
    QAbstractItemDelegate,
    QAbstractItemView,
    QStyledItemDelegate,
    QTableView,
    QWidget,
)

logger = logging.getLogger(__name__)

ContextMenuCallback = Callable[[QPoint], None]
DoubleClickCallback = Callable[[], None]
SortClickCallback = Callable[[Qt.SortOrder], None]


@dataclass(frozen=True, eq=True)
class Row:
    display_roles: List[str]
    edit_roles: List[Optional[Any]]
    background_roles: Dict[int, QBrush] = field(default_factory={})  # type: ignore
    change_callbacks: List[Optional[Callable[[Any], None]]] = field(
        hash=False, default_factory=lambda: [], compare=False
    )
    double_click_callback: Optional[DoubleClickCallback] = None


@dataclass(frozen=True, eq=True)
class Column:
    header_label: str
    editable: bool
    sorted_by: Optional[Qt.SortOrder] = None
    sort_click_callback: Optional[SortClickCallback] = None
    header_callback: Optional[ContextMenuCallback] = None


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

    def context_menu_callback(self, column: int) -> Optional[ContextMenuCallback]:
        return self._data.columns[column].header_callback

    def double_click_callback(self, row: int) -> Optional[DoubleClickCallback]:
        return self._data.rows[row].double_click_callback

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
        if role == Qt.BackgroundRole:
            return self._data.rows[index.row()].background_roles.get(
                index.column(), None
            )
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

    def sort(self, column: int, order: Qt.SortOrder = Qt.AscendingOrder) -> None:
        sort_click_cb = (
            self._data.columns[column].sort_click_callback if column >= 0 else None
        )
        if sort_click_cb is not None:
            sort_click_cb(order)


HeaderMenuCallback = Callable[[], None]


def _sort_row(column: int, row: Row) -> Any:
    return row.edit_roles[column]


class DeclarativeTable(QTableView):
    def __init__(
        self,
        data: Data,
        parent: Optional[QWidget] = None,
    ) -> None:
        super().__init__(parent)

        self.horizontalHeader().setContextMenuPolicy(Qt.CustomContextMenu)
        self.horizontalHeader().customContextMenuRequested.connect(self._context_menu)

        self._data = data
        self._prior_delegates: List[QAbstractItemDelegate] = []
        model = DeclarativeTableModel(data)
        self.setModel(model)
        self.verticalHeader().hide()
        self.setAlternatingRowColors(True)
        self.horizontalHeader().setStretchLastSection(True)
        self.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.doubleClicked.connect(self._double_click)
        model.cell_changed.connect(self._cell_changed)

        for row_idx, delegate in data.row_delegates.items():
            self.setItemDelegateForRow(row_idx, delegate)

        for row_idx, delegate in data.column_delegates.items():
            self.setItemDelegateForColumn(row_idx, delegate)

        self.setEditTriggers(QAbstractItemView.DoubleClicked)

    def _double_click(self, index: QModelIndex) -> None:
        callback = self.model().double_click_callback(index.row())
        if callback is not None:
            callback()

    def _context_menu(self, pos: QPoint) -> None:
        callback = self.model().context_menu_callback(self.indexAt(pos).column())
        if callback is not None:
            callback(self.mapToGlobal(pos))

    def _cell_changed(self, index: QModelIndex, new_value: Any, _role: int) -> None:
        cb = self._data.rows[index.row()].change_callbacks[index.column()]
        if cb is not None:
            cb(new_value)

    def set_data(self, data: Data) -> None:
        # When this is called, we might be in the middle of an edit
        # operation, so the editor is still open. The current delegate needs to
        # survive in this case to do cleanup work, so we store the previous delegates.
        # Shouldn't harm anyone.
        # You can check this case using isPersistentEditorOpen on all indices. It will be
        # true at the beginning of this function and false at the end.
        # If you don't store the previous delegates you will get a random segfault, because
        # Python will decide to kill the old delegate at some point.
        self._prior_delegates = list(self._data.row_delegates.values())

        for row_idx, delegate in self._data.row_delegates.items():
            # noinspection PyTypeChecker
            self.setItemDelegateForRow(row_idx, QStyledItemDelegate())  # type: ignore

        for row_idx, delegate in self._data.column_delegates.items():
            # noinspection PyTypeChecker
            self.setItemDelegateForColumn(row_idx, QStyledItemDelegate())  # type: ignore

        self.model().set_data(data)
        sort_column = next(
            iter(
                (idx, c.sorted_by)
                for idx, c in enumerate(data.columns)
                if c.sorted_by is not None
            ),
            None,
        )
        if sort_column is None:
            self.horizontalHeader().setSortIndicatorShown(False)
        else:
            self.horizontalHeader().setSortIndicator(sort_column[0], sort_column[1])
        self._data = data

        for row_idx, delegate in data.row_delegates.items():
            self.setItemDelegateForRow(row_idx, delegate)

        for row_idx, delegate in data.column_delegates.items():
            self.setItemDelegateForColumn(row_idx, delegate)

        self.resizeColumnsToContents()

    # Keep these commented out. Maybe we want to really delete the delegates when
    # we're done with them, instead of keeping them around like idiots.
    # The commitData signal is called, but I'm not using that information, yet.
    # def closeEditor(
    #     self, editor: QWidget, hint: QAbstractItemDelegate.EndEditHint
    # ) -> None:
    #     logger.info("close editor begin")
    #     super().closeEditor(editor, hint)
    #     logger.info("close editor end")
    #
    # def commitData(self, editor: QWidget) -> None:
    #     logger.info("commit data begin")
    #     super().commitData(editor)
    #     logger.info("commit data end")
