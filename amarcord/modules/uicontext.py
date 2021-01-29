from typing import List
from PyQt5 import QtWidgets


class UIContext:
    def __init__(self, argv: List[str]) -> None:
        self._app = QtWidgets.QApplication(argv)
        self._app.setApplicationName("AMARCORD")
        self._main_window = QtWidgets.QMainWindow()
        self._tabs = QtWidgets.QTabWidget(self._main_window)
        self._main_window.setCentralWidget(self._tabs)

    def register_tab(self, label: str, w: QtWidgets.QWidget) -> None:
        self._tabs.addTab(w, label)

    def exec_(self) -> None:
        self._main_window.show()
        self._app.exec_()
