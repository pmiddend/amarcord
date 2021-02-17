from PyQt5 import QtWidgets

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
