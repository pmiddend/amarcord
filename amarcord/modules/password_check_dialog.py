from typing import Optional

from PyQt5.QtWidgets import QDialog
from PyQt5.QtWidgets import QDialogButtonBox
from PyQt5.QtWidgets import QFormLayout
from PyQt5.QtWidgets import QHBoxLayout
from PyQt5.QtWidgets import QLabel
from PyQt5.QtWidgets import QLineEdit
from PyQt5.QtWidgets import QStyle
from PyQt5.QtWidgets import QVBoxLayout
from PyQt5.QtWidgets import QWidget


def password_check_dialog(
    title: str, description: str, parent: Optional[QWidget] = None
) -> Optional[str]:
    dialog = QDialog(parent)
    dialog.setWindowTitle(title)
    dialog_layout = QVBoxLayout(dialog)

    description_row = QHBoxLayout()
    dialog_layout.addLayout(description_row)

    icon_label = QLabel()
    icon_label.setPixmap(
        dialog.style().standardIcon(QStyle.SP_MessageBoxWarning).pixmap(64, 64)
    )
    description_row.addWidget(icon_label)
    description_row.addWidget(QLabel(description))

    form_layout = QFormLayout()
    dialog_layout.addLayout(form_layout)

    password_edit = QLineEdit()
    password_edit.setEchoMode(QLineEdit.Password)
    form_layout.addRow("Admin password:", password_edit)

    buttonBox = QDialogButtonBox(  # type: ignore
        QDialogButtonBox.Ok | QDialogButtonBox.Cancel
    )
    buttonBox.accepted.connect(dialog.accept)
    buttonBox.rejected.connect(dialog.reject)

    dialog_layout.addWidget(buttonBox)

    if dialog.exec() == QDialog.Rejected:
        return None

    return password_edit.text()
