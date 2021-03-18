from functools import partial

from PyQt5.QtWidgets import (
    QApplication,
    QVBoxLayout,
    QWidget,
)

from amarcord.qt.declarative_table import (
    Column,
    Data,
    DeclarativeTable,
    Row,
    SortOrder,
)

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


class MyClass:
    def __init__(self) -> None:
        self.sorted = [None, SortOrder.DESC]
        self.widget = DeclarativeTable(
            self._create_data(),
            parent=None,
        )

    def _create_data(self) -> Data:
        return Data(
            rows=[
                Row(display_roles=["a", "b"], edit_roles=[None, None]),
                Row(display_roles=["c", "d"], edit_roles=[None, None]),
            ],
            columns=[
                Column(
                    header_label="x",
                    editable=False,
                    sorted_by=self.sorted[0],
                    sort_click_callback=partial(self._sort_clicked, 0),
                ),
                Column(
                    header_label="y",
                    editable=False,
                    sorted_by=self.sorted[1],
                    sort_click_callback=partial(self._sort_clicked, 1),
                ),
            ],
            row_delegates={},
            column_delegates={},
        )

    def _sort_clicked(self, column: int) -> None:
        col = self.sorted[column]
        new_order = col.invert() if col is not None else SortOrder.ASC
        print(f"previous sorted: {self.sorted}, now sorting {column} by {new_order}")
        self.sorted = [None, None]
        self.sorted[column] = new_order
        self.widget.set_data(self._create_data())


c = MyClass()
root_layout.addWidget(c.widget)
root_layout.addStretch()

root.show()
app.exec_()

# print(new_attributo_dialog([RunProperty("foo")], None))
