from typing import List
from typing import TypeVar
from typing import Generic
from typing import Iterable
from typing import Dict
from typing import Any
from typing import Callable
from typing import Optional
import logging

from PyQt5 import QtWidgets
from PyQt5 import QtCore

from amarcord.query_parser import filter_by_query
from amarcord.query_parser import Query

T = TypeVar("T")
Row = Dict[T, Any]
GeneralTable = List[Row]
DataRetriever = Callable[[], GeneralTable]
MenuCallback = Callable[[QtCore.QPoint, T], None]

logger = logging.getLogger(__name__)


class GeneralModel(QtCore.QAbstractTableModel, Generic[T]):
    def __init__(
        self,
        enum_type_retriever: Callable[[], Iterable[T]],
        column_header_retriever: Callable[[], Dict[T, str]],
        column_visibility: List[T],
        column_converter: Callable[[T, int, Any], str],
        data_retriever: Optional[DataRetriever],
        parent: Optional[QtCore.QObject],
    ) -> None:
        super().__init__(parent)
        self._enum_type_retriever = enum_type_retriever
        self._enum_type = enum_type_retriever()
        self._column_header_retriever = column_header_retriever
        self._column_headers = column_header_retriever()
        self.column_visibility = column_visibility
        self._enum_to_index: Dict[T, int] = {
            v: k for k, v in enumerate(column_visibility)
        }
        self.index_to_enum: Dict[int, T] = {
            v: k for k, v in self._enum_to_index.items()
        }
        self._data = data_retriever() if data_retriever is not None else []
        self._filtered_data = self._data
        # pylint: disable=unnecessary-comprehension
        self._enum_values = [e for e in self._enum_type]
        self._column_converter = column_converter
        self._sort_column: Optional[T] = None
        self._sort_reverse = False
        self._filter_query: Optional[Query] = None
        self._data_retriever: Optional[DataRetriever] = None

    def get_filtered_column_values(self, c: T) -> List[Any]:
        return [d[c] for d in self._filtered_data if c in d]

    def get_column_values(self, c: T) -> List[Any]:
        return [d[c] for d in self._data if c in d]

    def set_column_visibility(self, column_visibility: List[T]) -> None:
        self.column_visibility = column_visibility
        # noinspection PyUnresolvedReferences
        self.layoutAboutToBeChanged.emit()  # type: ignore
        self._enum_to_index = {v: k for k, v in enumerate(column_visibility)}
        self.index_to_enum = {v: k for k, v in self._enum_to_index.items()}
        # noinspection PyUnresolvedReferences
        self.layoutChanged.emit()  # type: ignore

    def set_data_retriever(self, data_retriever: DataRetriever) -> None:
        self._data_retriever = data_retriever
        self.beginResetModel()
        self._data = self._data_retriever()
        self._filtered_data = self._data
        self.endResetModel()

    def refresh(self) -> None:
        self._enum_type = self._enum_type_retriever()
        self._column_headers = self._column_header_retriever()
        if self._data_retriever:
            self._data = self._data_retriever()
        self.set_filter_query(self._filter_query)

    def rowCount(self, _parent: QtCore.QModelIndex = QtCore.QModelIndex()) -> int:
        return len(self._filtered_data)

    def columnCount(self, _parent: QtCore.QModelIndex = QtCore.QModelIndex()) -> int:
        return len(self.column_visibility)

    def set_filter_query(self, q: Optional[Query]) -> None:
        self._filter_query = q
        self.beginResetModel()
        if self._filter_query is not None:
            self._filtered_data = [
                row
                for row in self._data
                if filter_by_query(
                    self._filter_query, {str(k).lower(): v for k, v in row.items()}
                )
            ]
        else:
            self._filtered_data = self._data
        self._resort()
        self.endResetModel()

    def _resort(self) -> None:
        if self._sort_column is not None:
            self._filtered_data.sort(reverse=self._sort_reverse, key=self._sort_key)  # type: ignore

    def _sort_key(self, row: Dict[T, Any]) -> Any:
        assert self._sort_column is not None
        return row[self._sort_column]

    def row(self, row_idx: int) -> Row:
        return self._filtered_data[row_idx]

    def headerData(
        self,
        section: int,
        orientation: QtCore.Qt.Orientation,
        role: int = QtCore.Qt.DisplayRole,
    ) -> QtCore.QVariant:
        if orientation == QtCore.Qt.Vertical or role != QtCore.Qt.DisplayRole:
            return QtCore.QVariant()
        return QtCore.QVariant(self._column_headers[self.index_to_enum[section]])

    def data(
        self,
        index: QtCore.QModelIndex,
        role: int = QtCore.Qt.DisplayRole,
    ) -> Any:
        if role not in (QtCore.Qt.DisplayRole, QtCore.Qt.EditRole):
            return QtCore.QVariant()
        v = self._filtered_data[index.row()].get(
            self.index_to_enum[index.column()], None
        )
        if v is None:
            return "None"
        if self._column_converter is not None:
            return self._column_converter(self.index_to_enum[index.column()], role, v)
        return str(v)

    def sort(
        self, column: int, order: QtCore.Qt.SortOrder = QtCore.Qt.AscendingOrder
    ) -> None:
        # noinspection PyUnresolvedReferences
        self.layoutAboutToBeChanged.emit()  # type: ignore
        self._sort_column = self.index_to_enum[column]
        self._sort_reverse = order == QtCore.Qt.DescendingOrder
        self._resort()
        # noinspection PyUnresolvedReferences
        self.layoutChanged.emit()  # type: ignore


class GeneralTableWidget(QtWidgets.QTableView, Generic[T]):
    row_double_click = QtCore.pyqtSignal(dict)
    data_changed = QtCore.pyqtSignal()

    def __init__(
        self,
        enum_type_retriever: Callable[[], Iterable[T]],
        column_header_retriever: Callable[[], Dict[T, str]],
        column_visibility: List[T],
        column_converter: Callable[[T, int, Any], str],
        data_retriever: Optional[DataRetriever],
        parent: Optional[QtWidgets.QWidget],
    ) -> None:
        super().__init__(parent)
        self.setAlternatingRowColors(True)
        self.verticalHeader().hide()
        self.horizontalHeader().setStretchLastSection(True)
        self.setSortingEnabled(True)
        self._model = GeneralModel[T](
            enum_type_retriever,
            column_header_retriever,
            column_visibility,
            column_converter,
            data_retriever,
            parent=None,
        )
        self.setModel(self._model)
        # noinspection PyUnresolvedReferences
        self.setSelectionBehavior(
            # pylint: disable=no-member
            QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows  # type: ignore
        )
        self.doubleClicked.connect(self._double_click)
        self._menu_callback: Optional[MenuCallback] = None

    def set_menu_callback(self, f: MenuCallback) -> None:
        self._menu_callback = f
        self.horizontalHeader().setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        self.horizontalHeader().customContextMenuRequested.connect(self._context_menu)

    def _double_click(self, index: QtCore.QModelIndex) -> None:
        self.row_double_click.emit(self._model.row(index.row()))

    @property
    def column_visibility(self) -> List[T]:
        return self._model.column_visibility

    def set_column_visibility(self, columns: List[T]) -> None:
        self._model.set_column_visibility(columns)

    def set_data_retriever(self, data_retriever: DataRetriever) -> None:
        self._model.set_data_retriever(data_retriever)
        self.resizeRowsToContents()
        self.resizeColumnsToContents()

    def set_filter_query(self, q: Query) -> None:
        self._model.set_filter_query(q)

    def refresh(self) -> None:
        self._model.refresh()
        self.resizeRowsToContents()
        self.resizeColumnsToContents()

    def _context_menu(self, pos: QtCore.QPoint) -> None:
        column = self.indexAt(pos).column()
        if self._menu_callback is not None:
            self._menu_callback(
                self.mapToGlobal(pos), self.model().index_to_enum[column]
            )

    def get_column_values(self, c: T) -> List[Any]:
        return self._model.get_column_values(c)

    def get_filtered_column_values(self, c: T) -> List[Any]:
        return self._model.get_filtered_column_values(c)
