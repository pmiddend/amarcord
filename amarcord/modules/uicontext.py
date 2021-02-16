from typing import List
from typing import Optional
import logging
from amarcord.qt.logging_handler import QtLoggingHandler
from PyQt5 import QtWidgets
from PyQt5 import QtCore
from PyQt5 import QtGui


class UIContext:
    def __init__(self, argv: List[str]) -> None:
        self._app = QtWidgets.QApplication(argv)
        self._app.setApplicationName("AMARCORD")
        self._main_window = QtWidgets.QMainWindow()
        splitter = QtWidgets.QSplitter(QtCore.Qt.Vertical)
        self._tabs = QtWidgets.QTabWidget(splitter)
        log_root = QtWidgets.QWidget(splitter)
        log_root_layout = QtWidgets.QVBoxLayout()
        log_root_layout.addWidget(QtWidgets.QLabel("Log:"))
        log_output = QtWidgets.QPlainTextEdit()
        log_output.setReadOnly(True)
        log_root_layout.addWidget(log_output)
        log_root.setLayout(log_root_layout)
        logging.getLogger().addHandler(QtLoggingHandler(log_output))
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 1)
        self._main_window.setCentralWidget(splitter)

    def register_tab(
        self, label: str, w: QtWidgets.QWidget, icon: Optional[QtGui.QIcon] = None
    ) -> int:
        if icon is not None:
            return self._tabs.addTab(w, icon, label)
        return self._tabs.addTab(w, label)

    def select_tab(self, idx: int) -> None:
        self._tabs.setCurrentIndex(idx)

    def style(self) -> QtWidgets.QStyle:
        return self._app.style()

    def icon(self, icon_name: str) -> QtGui.QIcon:
        return self._app.style().standardIcon(getattr(QtWidgets.QStyle, icon_name))

    def exec_(self) -> None:
        self._main_window.show()
        self._app.exec_()
