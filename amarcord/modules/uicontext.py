import logging
from typing import Callable
from typing import List
from typing import Optional

from PyQt5 import QtCore
from PyQt5 import QtGui
from PyQt5.QtWidgets import QAction
from PyQt5.QtWidgets import QApplication
from PyQt5.QtWidgets import QLabel
from PyQt5.QtWidgets import QMainWindow
from PyQt5.QtWidgets import QPlainTextEdit
from PyQt5.QtWidgets import QSplitter
from PyQt5.QtWidgets import QStyle
from PyQt5.QtWidgets import QTabWidget
from PyQt5.QtWidgets import QVBoxLayout
from PyQt5.QtWidgets import QWidget

from amarcord.qt.logging_handler import QtLoggingHandler


# def _shit(type, context, msg):
#     import os
#     import signal
#
#     os.kill(os.getpid(), signal.SIGUSR1)
#     print("SHIT " + msg)


class UIContext:
    def __init__(self, argv: List[str]) -> None:
        self._app = QApplication(argv)
        self._app.setApplicationName("AMARCORD")
        self._app.setApplicationDisplayName("AMARCORD")
        self._main_window = QMainWindow()
        splitter = QSplitter(QtCore.Qt.Vertical)
        self._tabs = QTabWidget(splitter)
        log_root = QWidget(splitter)
        log_root_layout = QVBoxLayout(log_root)
        log_root_layout.addWidget(QLabel("Log:"))
        log_output = QPlainTextEdit()
        log_output.setReadOnly(True)
        log_root_layout.addWidget(log_output)
        self._logging_handler = QtLoggingHandler(log_output)
        logging.getLogger().addHandler(self._logging_handler)
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 1)
        self._main_window.setCentralWidget(splitter)

        self._file_menu = self._main_window.menuBar().addMenu("&File")
        self._quit_action = QAction(
            self.style().standardIcon(QStyle.SP_DialogCloseButton),
            "&Exit",
            self._main_window,
        )
        # mypy complains that close returns a boolean or something
        self._quit_action.triggered.connect(self._main_window.close)  # type: ignore
        self._file_menu.addAction(self._quit_action)

        # Enable this if you want to gdb-debug weird assertion messages
        # qInstallMessageHandler(_shit)

    def set_application_suffix(self, suffix: str) -> None:
        self._app.setApplicationDisplayName("AMARCORD - " + suffix)

    def register_tab(
        self, label: str, w: QWidget, icon: Optional[QtGui.QIcon] = None
    ) -> int:
        if icon is not None:
            return self._tabs.addTab(w, icon, label)
        return self._tabs.addTab(w, label)

    def select_tab(self, idx: int) -> None:
        self._tabs.setCurrentIndex(idx)

    def style(self) -> QStyle:
        return self._app.style()

    def icon(self, icon_name: str) -> QtGui.QIcon:
        return self._app.style().standardIcon(getattr(QStyle, icon_name))

    def exec_(self) -> None:
        self._main_window.show()
        self._app.exec_()

    def add_menu_item(self, title: str, f: Callable[[], None]) -> None:
        a = QAction(title, self._main_window)
        a.triggered.connect(f)
        self._file_menu.insertAction(self._quit_action, a)

    def close(self) -> None:
        logging.getLogger().removeHandler(self._logging_handler)
        self._main_window.close()
        self._main_window.deleteLater()
