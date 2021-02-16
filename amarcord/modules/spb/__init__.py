from PyQt5 import QtWidgets

from amarcord.modules.spb.run_table import RunTable
from amarcord.modules.context import Context


def run_table(context: Context) -> QtWidgets.QWidget:
    return RunTable(context)
