from dataclasses import dataclass
from typing import List, Optional

from PyQt5 import QtWidgets


@dataclass(frozen=True)
class NewRunData:
    id: int
    sample_id: Optional[int]


def new_run_dialog(
    parent: Optional[QtWidgets.QWidget],
    highest_id: Optional[int],
    sample_ids: List[int],
) -> Optional[NewRunData]:
    dialog = QtWidgets.QDialog(parent)
    dialog_layout = QtWidgets.QVBoxLayout(dialog)

    buttonBox = QtWidgets.QDialogButtonBox(  # type: ignore
        QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel
    )
    buttonBox.accepted.connect(dialog.accept)
    buttonBox.rejected.connect(dialog.reject)

    if highest_id is not None:
        dialog_layout.addWidget(
            QtWidgets.QLabel(f"Highest run ID is <b>{highest_id}</b>")
        )

    form_layout = QtWidgets.QFormLayout()

    run_id = QtWidgets.QSpinBox()
    run_id.setMinimum(0)
    run_id.setMaximum(2 ** 30)
    run_id.setValue(highest_id + 1 if highest_id is not None else 1)

    sample_chooser = QtWidgets.QComboBox()
    sample_chooser.addItems([str(s) for s in sample_ids] + ["None"])

    form_layout.addRow("Run ID", run_id)
    form_layout.addRow("Sample", sample_chooser)

    dialog_layout.addLayout(form_layout)

    dialog_layout.addWidget(buttonBox)

    if dialog.exec() == QtWidgets.QDialog.Rejected:
        return None

    chosen_sample = sample_chooser.currentText()
    return NewRunData(
        id=run_id.value(),
        sample_id=int(chosen_sample) if chosen_sample != "None" else None,
    )
