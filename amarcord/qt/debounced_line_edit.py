from typing import Optional

from PyQt5 import QtCore
from PyQt5 import QtWidgets


class DebouncedLineEdit(QtWidgets.QLineEdit):
    debouncedTextChanged = QtCore.pyqtSignal(str)

    def __init__(self, parent: Optional[QtWidgets.QLineEdit] = None) -> None:
        super().__init__(parent)

        self._debounce = QtCore.QTimer()
        self._debounce.setInterval(500)
        self._debounce.setSingleShot(True)
        self._debounce.timeout.connect(self._debounced)

        self.textChanged.connect(self._debounce.start)

    def _debounced(self) -> None:
        self.debouncedTextChanged.emit(self.text())
