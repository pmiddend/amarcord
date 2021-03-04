import re
from typing import List, Optional, Tuple, Union, cast

from PyQt5.QtCore import QObject
from PyQt5.QtGui import QValidator

from amarcord.util import str_to_float


def parse_float_list(
    input_: str, elements: int
) -> Union[QValidator.State, List[float]]:
    parts = re.split(", *", input_)

    if parts and parts[-1] == "":
        return QValidator.Intermediate

    if len(parts) < elements:
        return QValidator.Intermediate

    if len(parts) > elements:
        return QValidator.Invalid

    floats = [str_to_float(f) for f in parts]
    return (
        cast(List[float], floats)
        if all(f is not None for f in floats)
        else QValidator.Invalid
    )


class NumberListValidator(QValidator):
    def __init__(self, elements: int, parent: Optional[QObject] = None) -> None:
        super().__init__(parent)

        self._elements = elements

    def validate(self, input_: str, pos: int) -> Tuple[QValidator.State, str, int]:
        if input_ == "":
            return QValidator.Acceptable, input_, pos
        floats = parse_float_list(input_, self._elements)
        if isinstance(floats, list):
            return QValidator.Acceptable, input_, pos
        return floats, input_, pos
