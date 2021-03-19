from typing import Optional

from PyQt5 import QtCore
from PyQt5 import QtGui
from PyQt5 import QtWidgets


class RectangleWidget(QtWidgets.QWidget):
    def __init__(
        self, color: QtGui.QColor, parent: Optional[QtWidgets.QWidget] = None
    ) -> None:
        super().__init__(parent)

        self._color = color
        font = QtWidgets.QApplication.font()
        metrics = QtGui.QFontMetrics(font)
        r = metrics.boundingRect("W").size()
        self._size = QtCore.QSize(r.height(), r.height())

    def paintEvent(self, _event: QtGui.QPaintEvent) -> None:
        p = QtGui.QPainter(self)

        rect = self.rect()
        height = rect.height()
        p.fillRect(
            QtCore.QRect(rect.topLeft(), QtCore.QSize(height, height)), self._color
        )

    def sizeHint(self) -> QtCore.QSize:
        return self._size
