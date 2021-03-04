import logging
from typing import Callable, Optional, Union

from PyQt5.QtCore import QVariant, pyqtSignal
from PyQt5.QtGui import QValidator
from PyQt5.QtWidgets import QLineEdit, QWidget

logger = logging.getLogger(__name__)


ValidatedInputValue = Union[str, None, QVariant]


class ValidatedLineEdit(QLineEdit):
    value_change = pyqtSignal(QVariant)

    def __init__(
        self,
        value: ValidatedInputValue,
        validator: QValidator,
        to_string: Callable[[QVariant], str],
        from_string: Callable[[str], QVariant],
        placeholder: Optional[str] = None,
        parent: Optional[QWidget] = None,
    ) -> None:
        super().__init__(parent)

        self._to_string = to_string
        self._from_string = from_string

        if isinstance(value, str):
            self.setText(value)
        elif value is None:
            self.setText("")
        else:
            self.setText(self._to_string(value))

        self.textEdited.connect(self._text_changed)
        self.setValidator(validator)
        if placeholder is not None:
            self.setPlaceholderText(placeholder)

    def _text_changed(self, new_text: str) -> None:
        if self.validator().validate(new_text, 0)[0] == QValidator.Intermediate:
            self.setStyleSheet("background-color: #ffb8b8")
            self.value_change.emit(new_text)
        else:
            self.setStyleSheet("")
            if self.text():
                self.value_change.emit(self._from_string(self.text()))
            else:
                self.value_change.emit(None)

    def valid_value(self) -> bool:
        return self.validator().validate(self.text(), 0)[0] == QValidator.Acceptable

    def set_value(self, value: QVariant) -> None:
        if isinstance(value, str):
            self.setText(value)
        elif value is None:
            self.setText("")
        else:
            self.setText(self._to_string(value))

        self._text_changed(self.text())

    def value(self) -> Optional[QVariant]:
        return self._from_string(self.text()) if self.text() else None
