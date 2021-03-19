from pathlib import Path
from typing import Any
from typing import Optional

from PyQt5.QtCore import QPoint
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QPainter
from PyQt5.QtGui import QPixmap
from PyQt5.QtWidgets import QDialog
from PyQt5.QtWidgets import QDialogButtonBox
from PyQt5.QtWidgets import QFrame
from PyQt5.QtWidgets import QLabel
from PyQt5.QtWidgets import QScrollArea
from PyQt5.QtWidgets import QVBoxLayout
from PyQt5.QtWidgets import QWidget


class PixmapLabel(QLabel):
    def __init__(self, img: str, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.setFrameStyle(QFrame.StyledPanel)
        self._pixmap = QPixmap(img)

    def paintEvent(self, _event: Any) -> None:
        size = self.size()
        painter = QPainter(self)
        point = QPoint(0, 0)
        scaledPix = self._pixmap.scaled(
            size, Qt.KeepAspectRatio, transformMode=Qt.SmoothTransformation
        )
        point.setX((size.width() - scaledPix.width()) // 2)
        point.setY((size.height() - scaledPix.height()) // 2)
        painter.drawPixmap(point, scaledPix)


def display_image_viewer(image_path: Path, parent: Optional[QWidget] = None) -> None:
    dialog = QDialog(parent)

    dialog_layout = QVBoxLayout()
    dialog.setLayout(dialog_layout)

    scroll_area = QScrollArea()
    dialog_layout.addWidget(scroll_area)

    image_label = PixmapLabel(str(image_path))

    scroll_layout = QVBoxLayout()
    scroll_area.setLayout(scroll_layout)

    scroll_layout.addWidget(image_label)

    buttonBox = QDialogButtonBox(QDialogButtonBox.Close)  # type: ignore
    buttonBox.accepted.connect(dialog.accept)
    buttonBox.rejected.connect(dialog.reject)
    dialog_layout.addWidget(buttonBox)

    dialog.show()
