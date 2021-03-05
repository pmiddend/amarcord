import logging
from typing import Callable, Optional, Tuple, Union

from PyQt5.QtCore import QObject, QVariant, pyqtSignal
from PyQt5.QtGui import QValidator
from PyQt5.QtWidgets import QLineEdit, QWidget

from amarcord.qt.validators import Partial

logger = logging.getLogger(__name__)


ValidatedInputValue = Union[Partial, None, QVariant]


class _Validator(QValidator):
    def __init__(
        self, f: Callable[[str], ValidatedInputValue], parent: Optional[QObject] = None
    ) -> None:
        super().__init__(parent)
        self._f = f

    def validate(self, input_: str, pos: int) -> Tuple[QValidator.State, str, int]:
        if input_ == "":
            return QValidator.Acceptable, input_, pos

        result = self._f(input_)

        if isinstance(result, Partial):
            return QValidator.Intermediate, result.returned_input, pos

        if result is None:
            return QValidator.Intermediate, input_, pos
            # return QValidator.Invalid, input_, pos

        return QValidator.Acceptable, input_, pos


class ValidatedLineEdit(QLineEdit):
    value_change = pyqtSignal(QVariant)

    def __init__(
        self,
        value: ValidatedInputValue,
        to_string: Callable[[QVariant], str],
        from_string: Callable[[str], ValidatedInputValue],
        placeholder: Optional[str] = None,
        parent: Optional[QWidget] = None,
    ) -> None:
        super().__init__(parent)

        self._to_string = to_string
        self._from_string = from_string

        if isinstance(value, Partial):
            self.setText(value.returned_input)
        elif value is None:
            self.setText("")
        else:
            self.setText(self._to_string(value))

        self.textEdited.connect(self._text_changed)

        self.setValidator(_Validator(from_string))
        if placeholder is not None:
            self.setPlaceholderText(placeholder)

    def _text_changed(self, new_text: str) -> None:
        if self.validator().validate(new_text, 0)[0] == QValidator.Intermediate:
            self.setStyleSheet("background-color: #ffb8b8")
            self.value_change.emit(Partial(new_text))
        else:
            self.setStyleSheet("")
            if self.text():
                self.value_change.emit(self._from_string(self.text()))
            else:
                self.value_change.emit(None)

    def valid_value(self) -> bool:
        return self.validator().validate(self.text(), 0)[0] == QValidator.Acceptable

    def set_value(self, value: Optional[QVariant]) -> None:
        if isinstance(value, Partial):
            self.setText(value.returned_input)
        elif value is None:
            self.setText("")
        else:
            self.setText(self._to_string(value))

        self._text_changed(self.text())

    def value(self) -> Optional[QVariant]:
        if not self.text():
            return None
        result = self._from_string(self.text())
        if isinstance(result, Partial):
            return None
        return result
