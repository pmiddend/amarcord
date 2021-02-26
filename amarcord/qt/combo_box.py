from typing import Generic, List, Optional, Tuple, TypeVar

from PyQt5.QtCore import QVariant, pyqtSignal
from PyQt5.QtWidgets import QComboBox, QWidget

T = TypeVar("T")


class ComboBox(QComboBox, Generic[T]):
    item_selected = pyqtSignal(QVariant)

    def __init__(
        self, items: List[Tuple[str, T]], parent: Optional[QWidget] = None
    ) -> None:
        super().__init__(parent)

        self._items = items
        self.currentIndexChanged.connect(self._index_changed)
        self.addItems([i[0] for i in items])

    def _index_changed(self, idx: int) -> None:
        self.item_selected.emit(QVariant(self._items[idx][1]))
