from typing import List
from typing import TypeVar
from typing import Generic
from typing import Iterable
from typing import Dict
from typing import Any
from typing import Callable
from typing import Optional

from enum import Enum

from PyQt5 import QtWidgets
from PyQt5 import QtCore

from amarcord.query_parser import filter_by_query
from amarcord.query_parser import Query

T = TypeVar("T")
Row = Dict[Enum, Any]
GeneralTable = List[Row]
DataRetriever = Callable[[], GeneralTable]


class GeneralModel(QtCore.QAbstractTableModel, Generic[T]):
    def __init__(
        self,
        enum_type: Iterable[T],
        column_headers: Dict[T, str],
        column_converters: Dict[T, Callable[[Any, int], Any]],
        data_retriever: Optional[DataRetriever],
        parent: Optional[QtCore.QObject],
    ) -> None:
        super().__init__(parent)
        self._enum_type = enum_type
        self._column_headers = column_headers
        self._index_to_enum: Dict[int, T] = {
            idx: v
            # pylint: disable=unnecessary-comprehension
            for idx, v in enumerate(e for e in enum_type)
        }
        self._enum_to_index: Dict[T, int] = {
            k: v for v, k in self._index_to_enum.items()
        }
        self._data = data_retriever() if data_retriever is not None else []
        self._filtered_data = self._data
        # pylint: disable=unnecessary-comprehension
        self._enum_values = [e for e in self._enum_type]
        self._column_converters = column_converters
        self._sort_column: Optional[T] = None
        self._sort_reverse = False

    def set_data_retriever(self, data_retriever: DataRetriever) -> None:
        self.beginResetModel()
        self._data = data_retriever()
        self._filtered_data = self._data
        self.endResetModel()

    def rowCount(self, parent: QtCore.QModelIndex = QtCore.QModelIndex()) -> int:
        return len(self._filtered_data)

    def columnCount(self, parent: QtCore.QModelIndex = QtCore.QModelIndex()) -> int:
        return len(self._enum_values)

    def set_filter_query(self, q: Query) -> None:
        self.beginResetModel()
        self._filtered_data = [
            row
            for row in self._data
            if filter_by_query(q, {k.name.lower(): v for k, v in row.items()})
        ]
        self._resort()
        self.endResetModel()

    def _resort(self) -> None:
        if self._sort_column is not None:
            self._filtered_data.sort(reverse=self._sort_reverse, key=self._sort_key)

    def _sort_key(self, row: Row) -> Any:
        return row[self._sort_column]

    def headerData(
        self,
        section: int,
        orientation: QtCore.Qt.Orientation,
        role: int = QtCore.Qt.DisplayRole,
    ) -> QtCore.QVariant:
        if orientation == QtCore.Qt.Vertical or role != QtCore.Qt.DisplayRole:
            return QtCore.QVariant()
        return QtCore.QVariant(self._column_headers[self._index_to_enum[section]])

    def data(
        self,
        index: QtCore.QModelIndex,
        role: int = QtCore.Qt.DisplayRole,
    ) -> Any:
        v = self._filtered_data[index.row()][self._index_to_enum[index.column()]]
        column_converter = self._column_converters.get(
            self._index_to_enum[index.column()], None
        )
        if column_converter is not None:
            return column_converter(v, role)
        if role in (QtCore.Qt.DisplayRole, QtCore.Qt.EditRole):
            return str(v)
        return QtCore.QVariant()

    def sort(
        self, column: int, order: QtCore.Qt.SortOrder = QtCore.Qt.AscendingOrder
    ) -> None:
        self.layoutAboutToBeChanged.emit()
        self._sort_column = self._index_to_enum[column]
        self._sort_reverse = order == QtCore.Qt.DescendingOrder
        self._resort()
        self.layoutChanged.emit()


class GeneralTableWidget(QtWidgets.QTableView, Generic[T]):
    def __init__(
        self,
        enum_type: Iterable[T],
        column_headers: Dict[T, str],
        column_converters: Dict[T, Callable[[Any, int], Any]],
        data_retriever: DataRetriever,
        parent: Optional[QtWidgets.QWidget],
    ) -> None:
        super().__init__(parent)
        self.setAlternatingRowColors(True)
        self.verticalHeader().hide()
        self.horizontalHeader().setStretchLastSection(True)
        self.setSortingEnabled(True)
        self._model = GeneralModel[T](
            enum_type,
            column_headers,
            column_converters,
            data_retriever,
            parent=None,
        )
        self.setModel(self._model)

    def set_data_retriever(self, data_retriever: DataRetriever) -> None:
        self._model.set_data_retriever(data_retriever)

    def set_filter_query(self, q: Query) -> None:
        self._model.set_filter_query(q)
