from typing import Dict, List, Optional

from PyQt5 import QtCore, QtWidgets

from amarcord.modules.spb.queries import RunPropertyMetadata, SPBQueries
from amarcord.modules.spb.run_property import RunProperty


def display_column_chooser(
    parent: Optional[QtWidgets.QWidget],
    selected_columns: List[RunProperty],
    queries: SPBQueries,
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
    with queries.connect() as conn:
        metadata_dict: Dict[
            RunProperty, RunPropertyMetadata
        ] = queries.run_property_metadata(conn)
        name_to_idx: Dict[RunProperty, int] = {}
        for idx, (prop, run_metadata) in enumerate(metadata_dict.items()):
            new_item = QtWidgets.QListWidgetItem(run_metadata.name)
            new_item.setData(QtCore.Qt.UserRole, str(prop))
            column_list.addItem(new_item)
            name_to_idx[prop] = idx
    for col in selected_columns:
        # -1 here because auto lets the enum start at 1 (which is fine actually)
        column_list.selectionModel().select(
            column_list.model().index(name_to_idx[col], 0),
            QtCore.QItemSelectionModel.Select,
        )
    root_layout.addWidget(column_list)
    buttonBox = QtWidgets.QDialogButtonBox(  # type: ignore
        QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel
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
        RunProperty(k.data(QtCore.Qt.UserRole)) for k in column_list.selectedItems()
    ]
