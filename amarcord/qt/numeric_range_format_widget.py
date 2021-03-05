from typing import Optional

from PyQt5.QtCore import QVariant, pyqtSignal
from PyQt5.QtWidgets import QLabel, QLineEdit, QVBoxLayout, QWidget
from lark import Tree, exceptions as le
from lark.lark import Lark
from lark.lexer import Token

from amarcord.numeric_range import NumericRange
from amarcord.query_parser import FieldNameError


class UnexpectedEOF(Exception):
    def __init__(self) -> None:
        super().__init__("Unexpected EOF")


def parse_range(s: str) -> NumericRange:
    try:
        (left_bound, left_value, _, right_value, right_bound) = _range_parser.parse(
            s
        ).children
        assert isinstance(left_bound, Tree) and isinstance(
            left_bound.children[0], Token
        )
        # noinspection PyUnresolvedReferences
        left_inclusive = left_bound.children[0].type == "LEFT_INCLUSIVE"
        assert isinstance(right_bound, Tree) and isinstance(
            right_bound.children[0], Token
        )
        # noinspection PyUnresolvedReferences
        right_inclusive = right_bound.children[0].type == "RIGHT_INCLUSIVE"
        assert isinstance(left_value, Tree) and isinstance(
            left_value.children[0], Token
        )
        # noinspection PyUnresolvedReferences
        left_value = (
            left_value.children[0].value  # type: ignore
            if left_value.children[0].type != "INFINITY"
            else None
        )
        # noinspection PyUnresolvedReferences
        assert isinstance(right_value, Tree) and isinstance(
            right_value.children[0], Token
        )
        # noinspection PyUnresolvedReferences
        right_value = (
            right_value.children[0].value  # type: ignore
            if right_value.children[0].type != "INFINITY"
            else None
        )
        # noinspection PyUnresolvedReferences
        return NumericRange(float(left_value) if left_value is not None else None, left_inclusive, float(right_value) if right_value is not None else None, right_inclusive)  # type: ignore
    except le.UnexpectedEOF:
        # pylint: disable=raise-missing-from
        raise UnexpectedEOF()
    except Exception as e:
        if e.__context__ and isinstance(e.__context__, FieldNameError):
            raise e.__context__
        raise e


class NumericRangeFormatWidget(QWidget):
    range_changed = pyqtSignal(QVariant)

    def __init__(
        self, numeric_range: Optional[NumericRange], parent: Optional[QWidget] = None
    ) -> None:
        super().__init__(parent)
        self._numeric_range = numeric_range
        self._input = QLineEdit(str(numeric_range) if numeric_range is not None else "")
        self._input.textChanged.connect(self._text_changed)
        self._input.setPlaceholderText("example: [3,4] or (oo, 3] or (0,1)")
        self._query_error = QLabel(self)
        self._query_error.setStyleSheet("QLabel { font: italic 10px; color: red; }")
        self._layout = QVBoxLayout()
        self._layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(self._layout)

        self._layout.addWidget(self._input)

    @property
    def numeric_range(self) -> Optional[NumericRange]:
        return self._numeric_range

    @numeric_range.setter
    def numeric_range(self, new_value: Optional[NumericRange]) -> None:
        self._numeric_range = new_value

    def _text_changed(self, new_text: str) -> None:
        old_range = self._numeric_range
        self._numeric_range = None
        try:
            self._numeric_range = parse_range(new_text)
            if self._query_error.text():
                self._layout.removeWidget(self._query_error)
        except UnexpectedEOF:
            self._query_error.setText("")
            self._layout.removeWidget(self._query_error)
        except Exception as e:
            error_before = bool(self._query_error.text())
            self._query_error.setText(f"{e}")
            if not error_before:
                self._layout.addWidget(self._query_error)
        if old_range != self._numeric_range:
            self.range_changed.emit(self._numeric_range)


_range_parser = Lark(
    r"""
!start: left_bound value COMMA value right_bound
!left_bound : LEFT_INCLUSIVE | LEFT_EXCLUSIVE
!right_bound : RIGHT_INCLUSIVE | RIGHT_EXCLUSIVE
!value: INFINITY | NUMBER

%import common.SIGNED_NUMBER    -> NUMBER

INFINITY: "oo"
LEFT_INCLUSIVE: "["
LEFT_EXCLUSIVE: "("
RIGHT_INCLUSIVE: "]"
RIGHT_EXCLUSIVE: ")"
COMMA: ","

%ignore " "
  """
)
