from typing import Iterable, Optional

from PyQt5.QtCore import QRegExp
from PyQt5.QtGui import QRegExpValidator
from PyQt5.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QLineEdit,
    QVBoxLayout,
    QWidget,
)

from amarcord.modules.spb.queries import CustomRunProperty
from amarcord.modules.spb.run_property import RunProperty
from amarcord.qt.combo_box import ComboBox


def new_custom_column_dialog(
    existing_properties: Iterable[RunProperty],
    parent: Optional[QWidget] = None,
) -> Optional[CustomRunProperty]:
    dialog = QDialog(parent)
    dialog_layout = QVBoxLayout()
    dialog.setLayout(dialog_layout)

    form = QFormLayout()
    name_input = QLineEdit()
    name_input.setValidator(QRegExpValidator(QRegExp(r"[a-zA-Z_][a-zA-Z_0-9]*")))
    form.addRow("Name", name_input)

    description_input = QLineEdit()

    form.addRow("Description", description_input)

    type_combo = ComboBox(
        [(f.description(), f.value) for f in CustomRunPropertyType],
    )
    form.addRow("Type", type_combo)
    dialog_layout.addLayout(form)

    button_box = QDialogButtonBox(  # type: ignore
        QDialogButtonBox.Ok | QDialogButtonBox.Cancel
    )
    button_box.accepted.connect(dialog.accept)
    button_box.rejected.connect(dialog.reject)

    button_box.button(QDialogButtonBox.Ok).setEnabled(False)

    def enable_ok(new_text: str) -> None:
        button_box.button(QDialogButtonBox.Ok).setEnabled(
            bool(new_text) and RunProperty(new_text) not in existing_properties
        )

    name_input.textChanged.connect(enable_ok)

    dialog_layout.addWidget(button_box)

    if dialog.exec() == QDialog.Rejected:
        return None

    return CustomRunProperty(
        name=RunProperty(name_input.text()),
        prop_type=CustomRunPropertyType(type_combo.current_value()),
        description=description_input.text(),
    )
