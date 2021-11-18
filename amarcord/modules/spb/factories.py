from typing import Optional
from typing import Set

import sqlalchemy as sa
from PyQt5.QtWidgets import QComboBox
from PyQt5.QtWidgets import QDialog
from PyQt5.QtWidgets import QDialogButtonBox
from PyQt5.QtWidgets import QGroupBox
from PyQt5.QtWidgets import QVBoxLayout
from PyQt5.QtWidgets import QWidget

from amarcord.db.proposal_id import ProposalId
from amarcord.db.tables import DBTables
from amarcord.modules.context import Context
from amarcord.modules.dbcontext import DBContext
from amarcord.modules.spb.overview_table import OverviewTable
from amarcord.modules.spb.run_details import RunDetails


def run_table(context: Context, table_data: DBTables, prop_id: ProposalId) -> QWidget:
    return OverviewTable(context, table_data, prop_id)


def run_details(context: Context, table_data: DBTables, prop_id: ProposalId) -> QWidget:
    return RunDetails(context, table_data, prop_id)


def create_proposal(context: DBContext, table_data: DBTables, proposal_id: int) -> None:
    with context.connect() as conn:
        conn.execute(
            sa.insert(table_data.proposal).values(id=proposal_id, admin_password=None)
        )


def retrieve_proposal_ids(context: DBContext, table_data: DBTables) -> Set[ProposalId]:
    with context.connect() as conn:
        return set(
            ProposalId(r[0])
            for r in conn.execute(sa.select([table_data.proposal.c.id])).fetchall()
        )


def proposal_chooser(proposal_ids: Set[ProposalId]) -> Optional[ProposalId]:
    dialog = QDialog()
    dialog_layout = QVBoxLayout(dialog)

    group_box = QGroupBox("Choose proposal:", dialog)
    dialog_layout.addWidget(group_box)
    root_layout = QVBoxLayout(group_box)

    proposal_combo = QComboBox()
    proposal_combo.addItems([str(s) for s in proposal_ids])

    root_layout.addWidget(proposal_combo)

    buttonBox = QDialogButtonBox(  # type: ignore
        QDialogButtonBox.Ok | QDialogButtonBox.Cancel
    )
    buttonBox.accepted.connect(dialog.accept)
    buttonBox.rejected.connect(dialog.reject)
    root_layout.addWidget(buttonBox)

    if dialog.exec() == QDialog.Rejected:
        return None
    return ProposalId(int(proposal_combo.currentText()))
