from typing import Set
from typing import Optional
from PyQt5 import QtWidgets
import sqlalchemy as sa
from amarcord.modules.spb.run_table import RunTable
from amarcord.modules.spb.run_details import RunDetails
from amarcord.db.tables import DBTables
from amarcord.db.proposal_id import ProposalId
from amarcord.modules.context import Context


def run_table(
    context: Context, table_data: DBTables, prop_id: ProposalId
) -> QtWidgets.QWidget:
    return RunTable(context, table_data, prop_id)


def run_details(
    context: Context, table_data: DBTables, prop_id: ProposalId
) -> QtWidgets.QWidget:
    return RunDetails(context, table_data, prop_id)


def retrieve_proposal_ids(context: Context, table_data: DBTables) -> Set[ProposalId]:
    with context.db.connect() as conn:
        return set(
            ProposalId(r[0])
            for r in conn.execute(sa.select([table_data.proposal.c.id])).fetchall()
        )


def proposal_chooser(proposal_ids: Set[ProposalId]) -> Optional[ProposalId]:
    dialog = QtWidgets.QDialog()
    dialog_layout = QtWidgets.QVBoxLayout(dialog)

    group_box = QtWidgets.QGroupBox("Choose proposal:", dialog)
    dialog_layout.addWidget(group_box)
    root_layout = QtWidgets.QVBoxLayout(group_box)

    proposal_combo = QtWidgets.QComboBox()
    proposal_combo.addItems([str(s) for s in proposal_ids])

    root_layout.addWidget(proposal_combo)

    buttonBox = QtWidgets.QDialogButtonBox(  # type: ignore
        QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel
    )
    buttonBox.accepted.connect(dialog.accept)
    buttonBox.rejected.connect(dialog.reject)
    root_layout.addWidget(buttonBox)

    if dialog.exec() == QtWidgets.QDialog.Rejected:
        return None
    return ProposalId(int(proposal_combo.currentText()))
