import logging
import time

from PyQt5 import QtWidgets


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
