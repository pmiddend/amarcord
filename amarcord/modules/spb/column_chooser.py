from enum import __call__
from typing import List, Optional

from PyQt5 import QtCore, QtWidgets

from amarcord.modules.spb.run_property import RunProperty, run_property_name


def display_column_chooser(
    parent: Optional[QtWidgets.QWidget], selected_columns: List[RunProperty]
) -> List[RunProperty]:
    dialog = QtWidgets.QDialog(parent)
    dialog_layout = QtWidgets.QVBoxLayout()
    dialog.setLayout(dialog_layout)
    root_widget = QtWidgets.QGroupBox("Choose which columns to display:")
    dialog_layout.addWidget(root_widget)
    root_layout = QtWidgets.QVBoxLayout()
    root_widget.setLayout(root_layout)
    column_list = QtWidgets.QListWidget()
    column_list.setSelectionMode(QtWidgets.QAbstractItemView.ExtendedSelection)
    for col in RunProperty:
        new_item = QtWidgets.QListWidgetItem(run_property_name(col))
        new_item.setData(QtCore.Qt.UserRole, col.value)
        column_list.addItem(new_item)
    for col in selected_columns:
        # -1 here because auto lets the enum start at 1 (which is fine actually)
        column_list.selectionModel().select(
            column_list.model().index(col.value - 1, 0),
            # pylint: disable=no-member
            QtCore.QItemSelectionModel.SelectionFlag.Select,  # type: ignore
        )
    root_layout.addWidget(column_list)
    buttonBox = QtWidgets.QDialogButtonBox(  # type: ignore
        QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel
    )
    buttonBox.accepted.connect(dialog.accept)
    buttonBox.rejected.connect(dialog.reject)
    root_layout.addWidget(buttonBox)

    def selection_changed(
        selected: QtCore.QItemSelection, deselected: QtCore.QItemSelection
    ) -> None:
        # pylint: disable=no-member
        buttonBox.button(QtWidgets.QDialogButtonBox.StandardButton.Ok).setEnabled(  # type: ignore
            bool(column_list.selectedItems())
        )

    column_list.selectionModel().selectionChanged.connect(selection_changed)  # type: ignore
    if dialog.exec() == QtWidgets.QDialog.Rejected:
        return selected_columns
    return [
        RunProperty(k.data(QtCore.Qt.UserRole)) for k in column_list.selectedItems()
    ]
