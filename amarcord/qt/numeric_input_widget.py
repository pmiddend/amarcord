import logging
from typing import Optional

from PyQt5.QtGui import QValidator
from PyQt5.QtWidgets import QLineEdit, QWidget

from amarcord.qt.numeric_range_format_widget import NumericRange
from amarcord.qt.numeric_range_validator import NumericRangeValidator

logger = logging.getLogger(__name__)


class NumericInputWidget(QLineEdit):
    def __init__(
        self,
        value: Optional[float],
        numeric_range: Optional[NumericRange],
        parent: Optional[QWidget] = None,
    ) -> None:
        super().__init__(parent)
        self._numeric_range = numeric_range
        logger.info("got a numeric range: %s", self._numeric_range)

        if value is not None:
            self.setText(str(value))
            assert self._numeric_range is None or self._numeric_range.value_is_inside(
                value
            )

        self.textEdited.connect(self._text_changed)
        self.setValidator(NumericRangeValidator(self._numeric_range))

    def _text_changed(self, new_text: str) -> None:
        if self.validator().validate(new_text, 0)[0] == QValidator.Intermediate:
            self.setStyleSheet("background-color: #ffb8b8")
        else:
            self.setStyleSheet("")

    def set_value(self, value: Optional[float]) -> None:
        if value is None:
            self.setText("")
        else:
            self.setText(str(value))

        self._text_changed(self.text())

    def value(self) -> Optional[float]:
        return float(self.text()) if self.text() else None
