from dataclasses import dataclass
from typing import Any
from typing import Callable
from typing import List
from typing import Optional

from PyQt5.QtCore import QAbstractItemModel
from PyQt5.QtCore import QModelIndex
from PyQt5.QtCore import QObject
from PyQt5.QtWidgets import QTreeView
from PyQt5.QtWidgets import QWidget


@dataclass(frozen=True)
class TreeNode:
    content: str
    expanded: bool
    current: bool
    children: List["TreeNode"]
    expand_callback: Callable[[], None]
    collapse_callback: Callable[[], None]
    current_callback: Callable[[], None]


@dataclass(frozen=True)
class Column:
    header: str
    stretch: bool


@dataclass(frozen=True)
class Data:
    nodes: TreeNode
    columns: List[Column]


class DeclarativeModel(QAbstractItemModel):
    def __init__(self, data: Data, parent: Optional[QObject] = None) -> None:
        super().__init__(parent)
        self._data = data

    def index(
        self, row: int, column: int, parent: QModelIndex = QModelIndex()
    ) -> QModelIndex:
        pass

    # def parent(self, child: QModelIndex) -> QModelIndex:
    #     pass

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        pass

    # def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
    #     return len(self._data.columns)
    #
    def data(self, index: QModelIndex, role: int = ...) -> Any:
        pass


class DeclarativeTree(QTreeView):
    def __init__(self, _data: Data, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
