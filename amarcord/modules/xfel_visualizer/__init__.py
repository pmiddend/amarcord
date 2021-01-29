from typing import Callable
from typing import List
from typing import Union
import time
import logging
from PyQt5 import QtWidgets
from PyQt5 import QtCore
from amarcord.modules.context import Context
from amarcord.sources.item import Item
from amarcord.python_schema import validate_dict
from amarcord.sources.mc import XFELMetadataCatalogue
from amarcord.sources.mc import XFELMetadataConnectionConfig
from amarcord.sources.mc import logger


class QtLoggingHandler(logging.Handler):
    def __init__(self, text_widget: QtWidgets.QPlainTextEdit) -> None:
        super().__init__()
        self._text_widget = text_widget

    def emit(self, record):
        self._text_widget.appendPlainText(
            "{} {}: {}".format(
                time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(record.created)),
                record.levelname,
                record.getMessage(),
            )
        )


class _GeneralVisualization(QtWidgets.QWidget):
    def __init__(
        self, process_logger: logging.Logger, on_reload: Callable[[], List[Item]]
    ) -> None:
        super().__init__()
        root_layout = QtWidgets.QHBoxLayout(self)
        splitter = QtWidgets.QSplitter(QtCore.Qt.Vertical, self)
        top_part = QtWidgets.QWidget()
        top_layout = QtWidgets.QVBoxLayout(top_part)
        top_bar_layout = QtWidgets.QHBoxLayout()
        reload_button = QtWidgets.QPushButton("Reload")
        reload_button.clicked.connect(lambda: self.reset_table(on_reload()))
        top_bar_layout.addWidget(reload_button)
        top_bar_layout.addItem(
            QtWidgets.QSpacerItem(
                40, 20, QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Minimum
            )
        )
        top_layout.addLayout(top_bar_layout)

        self._values_table = QtWidgets.QTableWidget()
        self._values_table.setColumnCount(2)
        self._values_table.setHorizontalHeaderItem(
            0, QtWidgets.QTableWidgetItem("Name")
        )
        self._values_table.setHorizontalHeaderItem(
            1, QtWidgets.QTableWidgetItem("Value")
        )
        self._values_table.horizontalHeader().setStretchLastSection(True)
        top_layout.addWidget(self._values_table)
        bottom_part = QtWidgets.QWidget()
        bottom_layout = QtWidgets.QVBoxLayout(bottom_part)
        log_heading = QtWidgets.QLabel("Log:")
        log_output = QtWidgets.QPlainTextEdit()
        log_output.setReadOnly(True)
        bottom_layout.addWidget(log_heading)
        bottom_layout.addWidget(log_output)
        splitter.addWidget(top_part)
        splitter.addWidget(bottom_part)
        root_layout.addWidget(splitter)
        process_logger.addHandler(QtLoggingHandler(log_output))
        self.reset_table(on_reload())

    def reset_table(self, items: List[Item]) -> None:
        self._values_table.setRowCount(len(items))
        self._values_table.setSortingEnabled(False)
        for row_idx, item in enumerate(items):
            self._values_table.setItem(
                row_idx, 0, QtWidgets.QTableWidgetItem(item.name)
            )
            self._values_table.setItem(
                row_idx, 1, QtWidgets.QTableWidgetItem(str(item.value))
            )
        self._values_table.setSortingEnabled(True)


class XFELVisualizer:
    def __init__(self, context: Context) -> None:
        if "mc" in context.config:
            mc_config: Union[List[str], XFELMetadataConnectionConfig] = validate_dict(
                context.config["mc"], XFELMetadataConnectionConfig
            )
            if isinstance(mc_config, list):
                raise Exception(f"Configuration invalid: {mc_config}")
            mc = XFELMetadataCatalogue(mc_config)
            context.ui.register_tab(
                "Metadata Catalogue",
                _GeneralVisualization(logger, lambda: mc.items("002252")),
            )
