from typing import Generic, List, Optional, Tuple, TypeVar, cast
import logging

from PyQt5.QtCore import QVariant, pyqtSignal
from PyQt5.QtWidgets import QComboBox, QWidget

from amarcord.qt.signal_blocker import SignalBlocker

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ComboBox(QComboBox, Generic[T]):
    item_selected = pyqtSignal(QVariant)

    def __init__(
        self, items: List[Tuple[str, T]], selected: T, parent: Optional[QWidget] = None
    ) -> None:
        super().__init__(parent)

        self.items = items
        self.addItems([i[0] for i in self.items])
        self.setCurrentIndex(cast(int, self._index_for_value(selected)))
        self.currentIndexChanged.connect(self._index_changed)

    def reset_items(self, new_items: List[Tuple[str, T]]):
        self.items = new_items
        last_value = self.current_value()
        with SignalBlocker(self):
            self.clear()
            self.addItems([i[0] for i in self.items])
            new_index = self._index_for_value(last_value)
            self.setCurrentIndex(0 if new_index is None else new_index)

    def _index_for_value(self, value: T) -> Optional[int]:
        return [x[1] for x in self.items].index(value)

    def set_current_value(self, new_value: T) -> None:
        new_index = self._index_for_value(new_value)
        assert new_index is not None, f"Element {new_value} is not in combobox"
        if new_index != self.currentIndex():
            self.setCurrentIndex(new_index)

    def current_value(self) -> T:
        return self.items[self.currentIndex()][1]

    def _index_changed(self, idx: int) -> None:
        self.item_selected.emit(QVariant(self.items[idx][1]))
