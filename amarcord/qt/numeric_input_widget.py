import logging
from typing import Optional
from typing import Union

from PyQt5.QtCore import QVariant
from PyQt5.QtCore import pyqtSignal
from PyQt5.QtGui import QValidator
from PyQt5.QtWidgets import QLineEdit
from PyQt5.QtWidgets import QWidget

from amarcord.qt.numeric_range_format_widget import NumericRange
from amarcord.qt.numeric_range_validator import NumericRangeValidator

logger = logging.getLogger(__name__)


NumericInputValue = Union[str, None, float]


class NumericInputWidget(QLineEdit):
    value_change = pyqtSignal(QVariant)

    def __init__(
        self,
        value: NumericInputValue,
        numeric_range: Optional[NumericRange],
        placeholder: Optional[str] = None,
        parent: Optional[QWidget] = None,
    ) -> None:
        super().__init__(parent)
        self._numeric_range = numeric_range

        if isinstance(value, str):
            self.setText(value)
        elif value is None:
            self.setText("")
        else:
            self.setText(str(value))

        self.textEdited.connect(self._text_changed)
        self.setValidator(NumericRangeValidator(self._numeric_range))
        if placeholder is not None:
            self.setPlaceholderText(placeholder)

    def _text_changed(self, new_text: str) -> None:
        if self.validator().validate(new_text, 0)[0] == QValidator.Intermediate:
            self.setStyleSheet("background-color: #ffb8b8")
            self.value_change.emit(new_text)
        else:
            self.setStyleSheet("")
            if self.text():
                self.value_change.emit(float(self.text()))
            else:
                self.value_change.emit(None)

    def valid_value(self) -> bool:
        return self.validator().validate(self.text(), 0)[0] == QValidator.Acceptable

    def set_value(self, value: NumericInputValue) -> None:
        if isinstance(value, str):
            self.setText(value)
        elif value is None:
            self.setText("")
            self.setStyleSheet("")
        else:
            self.setText(str(value))

    def value(self) -> Optional[float]:
        return float(self.text()) if self.text() else None
