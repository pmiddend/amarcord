from typing import Generic, List, Optional, Tuple, TypeVar
import logging

from PyQt5.QtCore import QVariant, pyqtSignal
from PyQt5.QtWidgets import QComboBox, QWidget

T = TypeVar("T")

logger = logging.getLogger(__name__)


class ComboBox(QComboBox, Generic[T]):
    item_selected = pyqtSignal(QVariant)

    def __init__(
        self, items: List[Tuple[str, T]], selected: T, parent: Optional[QWidget] = None
    ) -> None:
        super().__init__(parent)

        self._items = items
        self.addItems([i[0] for i in items])
        self.setCurrentIndex(self._index_for_value(selected))
        self.currentIndexChanged.connect(self._index_changed)

    def _index_for_value(self, value: T) -> int:
        return [x[1] for x in self._items].index(value)

    def set_current_value(self, new_value: T) -> None:
        self.setCurrentIndex(self._index_for_value(new_value))

    def current_value(self) -> T:
        return self._items[self.currentIndex()][1]

    def _index_changed(self, idx: int) -> None:
        self.item_selected.emit(QVariant(self._items[idx][1]))
