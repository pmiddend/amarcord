from typing import Optional
from typing import Tuple

from PyQt5.QtCore import QObject
from PyQt5.QtGui import QValidator

from amarcord.numeric_range import NumericRange


class NumericRangeValidator(QValidator):
    def __init__(
        self, numeric_range: Optional[NumericRange], parent: Optional[QObject] = None
    ) -> None:
        super().__init__(parent)
        self._numeric_range = numeric_range

    def validate(self, input_: str, pos: int) -> Tuple["QValidator.State", str, int]:
        if input_ == "":
            return QValidator.Acceptable, input_, pos
        if input_ == "-":
            return QValidator.Intermediate, input_, pos
        try:
            v = float(input_)
            if self._numeric_range is None or self._numeric_range.value_is_inside(v):
                return QValidator.Acceptable, input_, pos
            return QValidator.Intermediate, input_, pos
        except:
            return QValidator.Invalid, input_, pos

    # pylint: disable=no-self-use
    def fixup(self, input_: str) -> str:
        return input_
