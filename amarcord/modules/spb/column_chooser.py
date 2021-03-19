from typing import Dict
from typing import List
from typing import Optional

from PyQt5 import QtCore
from PyQt5 import QtWidgets
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QAbstractItemView
from PyQt5.QtWidgets import QDialog
from PyQt5.QtWidgets import QDialogButtonBox
from PyQt5.QtWidgets import QGroupBox
from PyQt5.QtWidgets import QListWidget
from PyQt5.QtWidgets import QListWidgetItem
from PyQt5.QtWidgets import QVBoxLayout
from PyQt5.QtWidgets import QWidget

from amarcord.db.tabled_attributo import TabledAttributo


def display_column_chooser(
    parent: Optional[QWidget],
    selected_columns: List[TabledAttributo],
    available_properties: List[TabledAttributo],
) -> List[TabledAttributo]:
    dialog = QDialog(parent)
    dialog_layout = QVBoxLayout(dialog)
    root_widget = QGroupBox("Choose which columns to display:")
    dialog_layout.addWidget(root_widget)
    root_layout = QVBoxLayout(root_widget)
    column_list = QListWidget()
    column_list.setSelectionMode(QAbstractItemView.ExtendedSelection)
    name_to_idx: Dict[str, int] = {}
    for idx, md in enumerate(available_properties):
        new_item = QListWidgetItem(md.pretty_id())
        new_item.setData(Qt.UserRole, md.technical_id())
        column_list.addItem(new_item)
        name_to_idx[md.technical_id()] = idx
    for col in selected_columns:
        # -1 here because auto lets the enum start at 1 (which is fine actually)
        column_list.selectionModel().select(
            column_list.model().index(name_to_idx[col.technical_id()], 0),
            QtCore.QItemSelectionModel.Select,
        )
    root_layout.addWidget(column_list)
    buttonBox = QDialogButtonBox(  # type: ignore
        QDialogButtonBox.Ok | QDialogButtonBox.Cancel
    )
    buttonBox.accepted.connect(dialog.accept)
    buttonBox.rejected.connect(dialog.reject)
    root_layout.addWidget(buttonBox)

    def selection_changed(
        _selected: QtCore.QItemSelection, _deselected: QtCore.QItemSelection
    ) -> None:
        buttonBox.button(QtWidgets.QDialogButtonBox.Ok).setEnabled(
            bool(column_list.selectedItems())
        )

    # noinspection PyUnresolvedReferences
    column_list.selectionModel().selectionChanged.connect(selection_changed)  # type: ignore
    if dialog.exec() == QtWidgets.QDialog.Rejected:
        return selected_columns
    return [
        available_properties[name_to_idx[k.data(Qt.UserRole)]]
        for k in column_list.selectedItems()
    ]
