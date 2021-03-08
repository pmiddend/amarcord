from PyQt5.QtWidgets import (
    QApplication,
    QVBoxLayout,
    QWidget,
)

from amarcord.qt.numeric_input_widget import NumericInputWidget
from amarcord.qt.numeric_range_format_widget import NumericRange

app = QApplication([])


# class MySpinBox(QAbstractSpinBox):
#     def __init__(
#         self, numeric_range: Optional[NumericRange], parent: Optional[QWidget] = None
#     ) -> None:
#         super().__init__(parent)
#         self._numeric_range = numeric_range
#
#         self.setButtonSymbols(QAbstractSpinBox.NoButtons)
#
#     def validate(self, input_: str, pos: int) -> Tuple[QValidator.State, str, int]:
#         if input_ == "":
#             return QValidator.Acceptable, input_, pos
#         if input_ == "-":
#             return QValidator.Intermediate, input_, pos
#         try:
#             v = float(input_)
#             if self._numeric_range is None or self._numeric_range.value_is_inside(v):
#                 return QValidator.Acceptable, input_, pos
#             return QValidator.Intermediate, input_, pos
#         except:
#             print("invalid")
#             return QValidator.Invalid, input_, pos
#
#     def fixup(self, input: str) -> str:


root = QWidget()
root_layout = QVBoxLayout()
root.setLayout(root_layout)

root_layout.addStretch()

widget = NumericInputWidget(None, NumericRange(0, True, 50, False))

root_layout.addWidget(widget)
root_layout.addStretch()

root.show()
app.exec_()

# print(new_attributo_dialog([RunProperty("foo")], None))
