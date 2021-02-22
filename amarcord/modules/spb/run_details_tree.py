import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from PyQt5 import QtWidgets


def _slot_tree_double_click(item: QtWidgets.QTreeWidgetItem) -> None:
    if item is None or not isinstance(item, _MyTreeWidgetItem):
        return

    d = item.additional_data

    if not isinstance(d, np.ndarray):
        return

    if len(d.shape) == 1:
        _data_shower(d, rows=0, columns=None)

    row_and_column = _table_layout_selection_dialog(d)

    if row_and_column is None:
        return

    _data_shower(d, rows=row_and_column[0], columns=row_and_column[1])


class RunDetailsTree(QtWidgets.QTreeWidget):
    def __init__(self, parent: Optional[QtWidgets.QWidget] = None) -> None:
        super().__init__(parent)

        self.setColumnCount(2)
        self.setHeaderLabels(["Key", "Type or value"])
        self.itemDoubleClicked.connect(_slot_tree_double_click)


def _data_shower(d: np.ndarray, rows: int, columns: Optional[int]) -> None:
    assert columns is None or columns < len(d.shape)
    assert rows < len(d.shape)
    dialog = QtWidgets.QDialog()
    dialog_layout = QtWidgets.QVBoxLayout()
    dialog.setLayout(dialog_layout)

    # plot_button = QtWidgets.QPushButton("Plot data")
    # plot_button.clicked.connect(plot)
    # dialog_layout.addWidget(plot_button)

    # tabs = QtWidgets.QtTabWidget(dialog)

    table = QtWidgets.QTableWidget()

    table.setRowCount(d.shape[rows])

    table.setColumnCount(d.shape[columns] if columns is not None else 1)

    if columns is None:
        for row in range(d.shape[rows]):
            table.setItem(row, 0, QtWidgets.QTableWidgetItem(str(d[row, 0])))
    else:
        for row in range(d.shape[rows]):
            for column in range(d.shape[columns]):
                table.setItem(
                    row, column, QtWidgets.QTableWidgetItem(str(d[row, column]))
                )

    table.resizeColumnsToContents()
    table.resizeRowsToContents()
    table.horizontalHeader().setStretchLastSection(True)
    # tabs.addTab(table, "Table")

    # plot = Plotter()
    # df = pd.DataFrame(
    #     {
    #         "x": d[]
    #         }
    # )
    # df.plot(ax=plot.axes())

    # tabs.addTab(plotter, "Plot")
    dialog_layout.addWidget(table)

    button_box = QtWidgets.QDialogButtonBox(  # type: ignore
        QtWidgets.QDialogButtonBox.Close
    )
    button_box.accepted.connect(dialog.accept)
    button_box.rejected.connect(dialog.reject)
    dialog_layout.addWidget(button_box)

    dialog.exec()


class _MyTreeWidgetItem(QtWidgets.QTreeWidgetItem):
    def __init__(
        self, additional_data: Any, parent: Optional[QtWidgets.QTreeWidgetItem]
    ) -> None:
        super().__init__(parent)
        self.additional_data = additional_data


class _TreeColumn(Enum):
    TREE_COLUMN_NAME = 0
    TREE_COLUMN_VALUE = 1


def _recurse_to_items(new_item: QtWidgets.QTreeWidgetItem, k: str, v: Any) -> None:
    if isinstance(v, dict):
        new_item.setText(_TreeColumn.TREE_COLUMN_VALUE.value, "")
        _dict_to_items(v, new_item)
    elif isinstance(v, list):
        new_item.setText(_TreeColumn.TREE_COLUMN_VALUE.value, "List")
        _list_to_items(v, new_item)
    elif isinstance(v, (bool, str, int, float)):
        if k == "timestamp":
            new_item.setText(
                _TreeColumn.TREE_COLUMN_VALUE.value,
                str(datetime.datetime.fromtimestamp(v / 1000 / 1000 / 1000)),
            )
        else:
            new_item.setText(_TreeColumn.TREE_COLUMN_VALUE.value, str(v))
    elif isinstance(v, np.ndarray):
        new_item.setText(_TreeColumn.TREE_COLUMN_VALUE.value, "Array " + str(v.shape))
    else:
        new_item.setText(_TreeColumn.TREE_COLUMN_VALUE.value, str(type(v)))


def _preprocess_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for k, v in d.items():
        if "." not in k:
            if isinstance(v, dict):
                result[k] = _preprocess_dict(v)
            else:
                result[k] = v
        else:
            parts = k.split(".")
            base = result
            for part in parts[:-1]:
                if part not in base:
                    base[part] = {}
                base = base[part]
            base[parts[-1]] = v
    return result


def _list_to_items(
    d: List[Any], parent: Optional[QtWidgets.QTreeWidgetItem]
) -> List[QtWidgets.QTreeWidgetItem]:
    items: List[QtWidgets.QTreeWidgetItem] = []
    for idx, v in enumerate(d):
        new_item = _MyTreeWidgetItem(v, parent)
        new_item.setText(_TreeColumn.TREE_COLUMN_NAME.value, str(idx))
        _recurse_to_items(new_item, str(idx), v)
        items.append(new_item)
    return items


def _dict_to_items(
    d: Dict[str, Any], parent: Optional[QtWidgets.QTreeWidgetItem]
) -> List[QtWidgets.QTreeWidgetItem]:
    items: List[QtWidgets.QTreeWidgetItem] = []
    for k, v in d.items():
        new_item = _MyTreeWidgetItem(v, parent)  # type: ignore
        new_item.setText(_TreeColumn.TREE_COLUMN_NAME.value, k)
        _recurse_to_items(new_item, k, v)
        items.append(new_item)
    return items


def _filter_dict(d: Dict[str, Any], filter_text: str) -> Dict[str, Any]:
    if not filter_text:
        return d

    def _keep_subtree(sd: Dict[str, Any]) -> bool:
        for k, v in sd.items():
            if filter_text.lower() in k.lower():
                return True
            if isinstance(v, dict) and _keep_subtree(v):
                return True
        return False

    new_dict: Dict[str, Any] = {}
    for k, v in d.items():
        if filter_text.lower() in k.lower():
            new_dict[k] = v

        if isinstance(v, dict) and _keep_subtree(v):
            new_dict[k] = _filter_dict(v, filter_text)
    return new_dict


def _table_layout_selection_dialog(d: np.ndarray) -> Optional[Tuple[int, int]]:
    dialog = QtWidgets.QDialog()
    dialog_layout = QtWidgets.QVBoxLayout()
    dialog.setLayout(dialog_layout)

    group_box = QtWidgets.QGroupBox("Choose columns and rows:", dialog)
    dialog_layout.addWidget(group_box)
    root_layout = QtWidgets.QFormLayout()
    group_box.setLayout(root_layout)

    rows_combo = QtWidgets.QComboBox()
    for idx, a in enumerate(d.shape):
        rows_combo.addItem(f"Dimension {idx} ({a} item" + ("s" if a > 1 else "") + ")")

    columns_combo = QtWidgets.QComboBox()
    for idx, a in enumerate(d.shape):
        columns_combo.addItem(
            f"Dimension {idx} ({a} item" + ("s" if a > 1 else "") + ")"
        )
    columns_combo.setCurrentIndex(1)

    root_layout.addRow(QtWidgets.QLabel("Rows:"), rows_combo)
    root_layout.addRow(QtWidgets.QLabel("Columns:"), columns_combo)

    button_box = QtWidgets.QDialogButtonBox(  # type: ignore
        QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel
    )
    button_box.accepted.connect(dialog.accept)
    button_box.rejected.connect(dialog.reject)
    root_layout.addWidget(button_box)

    def index_changed() -> None:
        # pylint: disable=no-member
        # noinspection PyUnresolvedReferences
        button_box.button(QtWidgets.QDialogButtonBox.StandardButton.Ok).setEnabled(  # type: ignore
            rows_combo.currentIndex() != columns_combo.currentIndex()
        )

    rows_combo.currentIndexChanged.connect(index_changed)
    columns_combo.currentIndexChanged.connect(index_changed)

    if dialog.exec() == QtWidgets.QDialog.Rejected:
        return None

    return rows_combo.currentIndex(), columns_combo.currentIndex()
