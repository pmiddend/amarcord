from functools import partial
from typing import List

from PyQt5 import QtCore
from PyQt5.QtWidgets import QCheckBox
from PyQt5.QtWidgets import QComboBox
from PyQt5.QtWidgets import QHBoxLayout
from PyQt5.QtWidgets import QHeaderView
from PyQt5.QtWidgets import QLineEdit
from PyQt5.QtWidgets import QPushButton
from PyQt5.QtWidgets import QWidget


class FilterHeader(QHeaderView):
    filterActivated = QtCore.pyqtSignal()
    changebuttonsymbol = QtCore.pyqtSignal()

    def __init__(self, parent: QWidget) -> None:
        super().__init__(QtCore.Qt.Horizontal, parent)
        self._editors: List[QWidget] = []
        self._padding = 0
        self.setStretchLastSection(True)
        # self.setResizeMode(QHeaderView.Stretch)
        self.setDefaultAlignment(QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter)
        # self.setSortIndicatorShown(False)
        self.sectionResized.connect(self.adjustPositions)
        parent.horizontalScrollBar().valueChanged.connect(self.adjustPositions)
        self.my_parent = parent

    def setFilterBoxes(self, count: int):
        # Delete present editors (move to function)
        while self._editors:
            editor = self._editors.pop()
            editor.deleteLater()
        for index in range(count):
            if index == 1:  # Empty
                editor = QWidget()
            elif index == 2:  # Number filter (>|=|<)
                editor = QWidget(self.my_parent)
                edlay = QHBoxLayout()
                # edlay.setContentsMargins(0, 0, 0, 0)
                # edlay.setSpacing(0)
                self.btn = QPushButton()
                self.btn.setText("=")
                self.btn.setFixedWidth(20)
                # self.btn.clicked.connect(lambda: self.changebuttonsymbol.emit(self.btn))
                self.btn.clicked.connect(self.changebuttonsymbol.emit)
                # btn.setViewportMargins(0, 0, 0, 0)
                linee = QLineEdit(self.parent())
                linee.setPlaceholderText("Filter")
                linee.returnPressed.connect(self.filterActivated.emit)
                # linee.setViewportMargins(0, 0, 0, 0)
                edlay.addWidget(self.btn)
                edlay.addWidget(linee)
                editor.setLayout(edlay)
            elif index == 3:
                editor = QComboBox(self.parent())
                editor.addItems(["", "Combo", "One", "Two", "Three"])
                editor.currentIndexChanged.connect(self.filterActivated.emit)
            elif index == 4:
                editor = QPushButton(self.parent())
                editor.clicked.connect(self.filterActivated.emit)
                editor.setText("Button")
            elif index == 5:
                editor = QCheckBox(self.parent())
                editor.clicked.connect(self.filterActivated.emit)
                editor.setTristate(True)
                editor.setCheckState(1)
                editor.setText("CheckBox")
            else:  # string filter
                editor = QWidget(self.my_parent)
                edlay = QHBoxLayout()
                editorLine = QLineEdit(self.parent())
                editorLine.setPlaceholderText("Filter")
                editorLine.returnPressed.connect(self.filterActivated.emit)
                editorLine.textChanged.connect(partial(self._textChanged, index))
                edlay.addWidget(editorLine)
                editor.setLayout(edlay)
            self._editors.append(editor)
        self.adjustPositions()

    def _textChanged(self, idx: int, new_text: str) -> None:
        print("text for index ", idx, " changed to ", new_text)

    def _height(self) -> int:
        return max((e.sizeHint().height() for e in self._editors), default=0)

    def sizeHint(self):
        size = super().sizeHint()
        if self._editors:
            size.setHeight(size.height() + self._height() + self._padding)
        return size

    def updateGeometries(self):
        if self._editors:
            self.setViewportMargins(0, 0, 0, self._height() + self._padding)
        else:
            self.setViewportMargins(0, 0, 0, 0)
        super().updateGeometries()
        self.adjustPositions()

    def adjustPositions(self):
        after_super_header = super().sizeHint().height()
        filter_header_height = self._height()
        for index, editor in enumerate(self._editors):
            height = editor.sizeHint().height()
            CompensateY = 0
            CompensateX = 0
            # if self._editors[index].__class__.__name__ == "QComboBox":
            #     CompensateY = +2
            # elif self._editors[index].__class__.__name__ == "QWidget":
            #     CompensateY = -1
            # elif self._editors[index].__class__.__name__ == "QPushButton":
            #     CompensateY = -1
            # elif self._editors[index].__class__.__name__ == "QCheckBox":
            #     CompensateY = 4
            #     CompensateX = 4
            editor.move(
                self.sectionPosition(index) - self.offset() + 1 + CompensateX,
                after_super_header
                + filter_header_height // 2
                - height // 2
                + 2
                + CompensateY,
            )
            editor.resize(self.sectionSize(index), height)
            # editor.move(
            #     self.sectionPosition(index) - self.offset() + 1 + CompensateX,
            #     height + (self._padding // 2) + 2 + CompensateY,
            # )
            # editor.resize(self.sectionSize(index), height)

    def filterText(self, index):
        if 0 <= index < len(self._editors):
            if self._editors[index].__class__.__name__ == "QLineEdit":
                return self._editors[index].text()
        return ""

    def setFilterText(self, index, text):
        if 0 <= index < len(self._editors):
            self._editors[index].setText(text)

    def clearFilters(self):
        for editor in self._editors:
            editor.clear()
