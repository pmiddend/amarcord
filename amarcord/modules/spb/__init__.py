from typing import Set
from typing import Optional
from PyQt5 import QtWidgets
import sqlalchemy as sa
from amarcord.modules.spb.run_table import RunTable
from amarcord.modules.spb.run_details import RunDetails
from amarcord.modules.spb.tables import Tables
from amarcord.modules.context import Context


def run_table(context: Context, tables: Tables, proposal_id: str) -> QtWidgets.QWidget:
    return RunTable(context, tables, proposal_id)


def run_details(
    context: Context, tables: Tables, proposal_id: str
) -> QtWidgets.QWidget:
    return RunDetails(context, tables, proposal_id)


def retrieve_proposal_ids(context: Context, tables: Tables) -> Set[str]:
    with context.db.connect() as conn:
        return set(
            r[0] for r in conn.execute(sa.select([tables.proposal.c.id])).fetchall()
        )


def proposal_chooser(proposal_ids: Set[str]) -> Optional[str]:
    dialog = QtWidgets.QDialog()
    dialog_layout = QtWidgets.QVBoxLayout()
    dialog.setLayout(dialog_layout)

    group_box = QtWidgets.QGroupBox("Choose proposal:", dialog)
    dialog_layout.addWidget(group_box)
    root_layout = QtWidgets.QVBoxLayout()

    group_box.setLayout(root_layout)

    proposal_combo = QtWidgets.QComboBox()
    proposal_combo.addItems(list(proposal_ids))

    root_layout.addWidget(proposal_combo)

    buttonBox = QtWidgets.QDialogButtonBox(  # type: ignore
        QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel
    )
    buttonBox.accepted.connect(dialog.accept)
    buttonBox.rejected.connect(dialog.reject)
    root_layout.addWidget(buttonBox)

    if dialog.exec() == QtWidgets.QDialog.Rejected:
        return None
    return proposal_combo.currentText()
