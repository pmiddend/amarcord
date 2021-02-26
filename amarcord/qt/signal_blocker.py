from PyQt5.QtWidgets import QWidget


class SignalBlocker:
    def __init__(self, widget: QWidget) -> None:
        self._widget = widget

    def __enter__(self) -> QWidget:
        self._widget.blockSignals(True)
        return self._widget

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._widget.blockSignals(False)
