from dataclasses import dataclass
from typing import Optional
import lark.exceptions as le
from PyQt5.QtCore import QVariant, pyqtSignal

from PyQt5.QtWidgets import QLabel, QLineEdit, QVBoxLayout, QWidget
from lark import Lark, Token, Tree

from amarcord.query_parser import FieldNameError


@dataclass(frozen=True, eq=True)
class NumericRange:
    minimum: Optional[float]
    minimum_inclusive: bool
    maximum: Optional[float]
    maximum_inclusive: bool


def empty_range() -> NumericRange:
    return NumericRange(None, False, None, False)


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
        return NumericRange(left_value, left_inclusive, right_value, right_inclusive)  # type: ignore
    except le.UnexpectedEOF as e:
        # pylint: disable=raise-missing-from
        raise UnexpectedEOF()
    except Exception as e:
        if e.__context__ and isinstance(e.__context__, FieldNameError):
            raise e.__context__
        raise e


class NumericRangeWidget(QWidget):
    range_changed = pyqtSignal(QVariant)

    def __init__(
        self, numeric_range: Optional[NumericRange], parent: Optional[QWidget] = None
    ) -> None:
        super().__init__(parent)
        self._numeric_range = numeric_range
        self._input = QLineEdit()
        self._input.textChanged.connect(self._text_changed)
        self._query_error = QLabel()
        self._query_error.setStyleSheet("QLabel { font: italic 10px; color: red; }")
        self._layout = QVBoxLayout()
        self.setLayout(self._layout)

        self._layout.addWidget(self._input)
        self._layout.addWidget(self._query_error)

    @property
    def numeric_range(self) -> Optional[NumericRange]:
        return self._numeric_range

    @numeric_range.setter
    def numeric_range(self, new_value: Optional[NumericRange]) -> None:
        self._numeric_range = new_value

    def _text_changed(self, new_text: str) -> None:
        self._numeric_range = None
        try:
            self._numeric_range = parse_range(new_text)
        except UnexpectedEOF:
            self._query_error.setText("")
        except Exception as e:
            self._query_error.setText(f"{e}")
        self.range_changed.emit(self._numeric_range)
