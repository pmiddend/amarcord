from dataclasses import dataclass
from typing import Optional, List
from PyQt5 import QtWidgets
from amarcord.modules.spb.proposal_id import ProposalId
from amarcord.modules.spb.run_id import RunId
from amarcord.modules.spb.db import DB


@dataclass(frozen=True)
class NewRunData:
    id: RunId
    sample_id: Optional[int]


def _new_run_dialog(
    parent: Optional[QtWidgets.QWidget],
    highest_id: Optional[RunId],
    sample_ids: List[int],
) -> Optional[NewRunData]:
    dialog = QtWidgets.QDialog(parent)
    dialog_layout = QtWidgets.QVBoxLayout()
    dialog.setLayout(dialog_layout)

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
    run_id.setValue(RunId(highest_id + 1) if highest_id is not None else 1)

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
        id=RunId(run_id.value()),
        sample_id=int(chosen_sample) if chosen_sample != "None" else None,
    )


def new_run_dialog(
    parent: Optional[QtWidgets.QWidget],
    proposal_id: ProposalId,
    db: DB,
) -> Optional[RunId]:
    with db.dbcontext.connect() as conn:
        new_run = _new_run_dialog(
            parent=parent,
            highest_id=max(
                (r for r in db.retrieve_run_ids(conn, proposal_id)),
                default=None,
            ),
            sample_ids=db.retrieve_sample_ids(conn),
        )

    if new_run is None:
        return None

    with db.dbcontext.connect() as conn:
        db.create_run(conn, proposal_id, new_run.id, new_run.sample_id)
        return new_run.id
