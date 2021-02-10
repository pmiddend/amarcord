from typing import Callable
from typing import Optional
from typing import List
from typing import Union
import time
import logging
from PyQt5 import QtWidgets
from PyQt5 import QtCore
from amarcord.modules.context import Context
from amarcord.qt.filter_header import FilterHeader
from amarcord.qt.tags import Tags


class MockModel(QtCore.QAbstractTableModel):
    def __init__(self, parent: Optional[QtCore.QObject] = None) -> None:
        super().__init__(parent)

    def rowCount(self, parent: QtCore.QModelIndex = QtCore.QModelIndex()) -> int:
        return 2

    def columnCount(self, parent: QtCore.QModelIndex = QtCore.QModelIndex()) -> int:
        return 3

    def data(
        self,
        index: QtCore.QModelIndex,
        role: int = QtCore.Qt.DisplayRole,
    ) -> QtCore.QVariant:
        if role == QtCore.Qt.DisplayRole:
            a = [
                ["row 1 col 1", "row 1 col 2", "row 1 col 3"],
                ["row 2 col 1", "row 2 col 2", "row 2 col 3"],
            ]
            return QtCore.QVariant(a[index.row()][index.column()])
        return QtCore.QVariant()


class RunTable(QtWidgets.QWidget):
    def __init__(self, context: Context) -> None:
        super().__init__()

        # Init main widgets
        choose_columns = QtWidgets.QPushButton("Choose columns")
        open_inspector = QtWidgets.QPushButton("Open inspector")

        self._table_view = QtWidgets.QTableView()
        # header = FilterHeader(self._table_view)
        # header.setFilterBoxes(self._table_model.columnCount())
        # header.filterActivated.connect(self.handleFilterActivated)
        # self._table_view.setHorizontalHeader(header)
        self._table_model = MockModel()
        self._table_view.setModel(self._table_model)
        self._table_view.verticalHeader().hide()
        self._table_view.setColumnHidden(1, True)

        log_output = QtWidgets.QPlainTextEdit()
        log_output.setReadOnly(True)

        # Layouting stuff
        root_layout = QtWidgets.QVBoxLayout(self)
        header_layout = QtWidgets.QHBoxLayout()
        header_layout.addWidget(choose_columns)
        header_layout.addWidget(open_inspector)

        tags = Tags(self)
        tags.completion(["foo", "bar"])
        header_layout.addWidget(tags)
        root_layout.addLayout(header_layout)

        root_layout.addWidget(self._table_view)

        root_layout.addWidget(QtWidgets.QLabel("Log:"))
        root_layout.addWidget(log_output)

    def handleFilterActivated(self):
        header = self.view.horizontalHeader()
        for index in range(header.count()):
            if index != 4:
                print(index, header.filterText(index))
            else:
                print("Button")
