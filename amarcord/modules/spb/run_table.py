from typing import Optional
from typing import Dict
from typing import Final
from typing import Any
from typing import List
from typing import Set
from itertools import groupby
import logging
from enum import Enum, auto
import sqlalchemy as sa
import pandas as pd
from PyQt5 import QtWidgets
from PyQt5 import QtCore
from matplotlib.backends.backend_qt5agg import (
    FigureCanvasQTAgg,
    NavigationToolbar2QT as NavigationToolbar,
)
from matplotlib.figure import Figure
from amarcord.modules.context import Context
from amarcord.modules.dbcontext import DBContext
from amarcord.modules.spb.tables import Tables
from amarcord.qt.infix_completer import InfixCompletingLineEdit
from amarcord.qt.table import GeneralTableWidget
from amarcord.query_parser import parse_query
from amarcord.query_parser import UnexpectedEOF

logger = logging.getLogger(__name__)


class Column(Enum):
    RUN_ID = auto()
    STATUS = auto()
    SAMPLE = auto()
    STARTED = auto()
    REPETITION_RATE = auto()
    TAGS = auto()
    PULSE_ENERGY = auto()


def _column_query_names() -> Set[str]:
    return {f.name.lower() for f in Column}


Row = Dict[Column, Any]


def _retrieve_data_no_connection(
    context: DBContext, tables: Tables, proposal_id: str
) -> List[Row]:
    with context.connect() as conn:
        return _retrieve_runs(tables, conn, proposal_id)


def _retrieve_runs(tables: Tables, conn: Any, proposal_id: str) -> List[Row]:
    run = tables.run
    tag = tables.run_tag

    select_stmt = (
        sa.select(
            [
                run.c.id,
                run.c.status,
                run.c.sample_id,
                run.c.repetition_rate_mhz,
                run.c.pulse_energy_mj,
                tag.c.tag_text,
                run.c.started,
            ]
        )
        .select_from(run.outerjoin(tag))
        .where(run.c.proposal_id == proposal_id)
        .order_by(run.c.id)
    )
    result: List[Row] = []
    for _run_id, run_rows in groupby(
        conn.execute(select_stmt).fetchall(), lambda x: x[0]
    ):
        rows = list(run_rows)
        first_row = rows[0]
        result.append(
            {
                Column.RUN_ID: first_row[0],
                Column.STATUS: first_row[1],
                Column.SAMPLE: first_row[2],
                Column.REPETITION_RATE: first_row[3],
                Column.PULSE_ENERGY: first_row[4],
                Column.TAGS: set(row[5] for row in rows if row[5] is not None),
                Column.STARTED: first_row[6],
            }
        )
    return result


def _convert_tag_column(value: Set[str], role: int) -> Any:
    if role == QtCore.Qt.DisplayRole:
        return ", ".join(value)
    if role == QtCore.Qt.EditRole:
        return value
    return QtCore.QVariant()


def _column_header(c: Column) -> str:
    d = {
        Column.RUN_ID: "Run",
        Column.STATUS: "Status",
        Column.SAMPLE: "Sample",
        Column.REPETITION_RATE: "Repetition Rate",
        Column.PULSE_ENERGY: "Pulse Energy",
        Column.TAGS: "Tags",
        Column.STARTED: "Started",
    }
    return d[c]


_unplottable_columns: Final = set(
    {Column.RUN_ID, Column.STATUS, Column.SAMPLE, Column.TAGS}
)


def _display_column_chooser(
    parent: Optional[QtWidgets.QWidget], selected_columns: List[Column]
) -> List[Column]:
    dialog = QtWidgets.QDialog(parent)
    dialog_layout = QtWidgets.QVBoxLayout()
    dialog.setLayout(dialog_layout)
    root_widget = QtWidgets.QGroupBox("Choose which columns to display:")
    dialog_layout.addWidget(root_widget)
    root_layout = QtWidgets.QVBoxLayout()
    root_widget.setLayout(root_layout)
    column_list = QtWidgets.QListWidget()
    column_list.setSelectionMode(QtWidgets.QAbstractItemView.ExtendedSelection)
    for col in Column:
        new_item = QtWidgets.QListWidgetItem(_column_header(col))
        new_item.setData(QtCore.Qt.UserRole, col.value)
        column_list.addItem(new_item)
    for col in selected_columns:
        # -1 here because auto lets the enum start at 1 (which is fine actually)
        column_list.selectionModel().select(
            column_list.model().index(col.value - 1, 0),
            # pylint: disable=no-member
            QtCore.QItemSelectionModel.SelectionFlag.Select,  # type: ignore
        )
    root_layout.addWidget(column_list)
    buttonBox = QtWidgets.QDialogButtonBox(  # type: ignore
        QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel
    )
    buttonBox.accepted.connect(dialog.accept)
    buttonBox.rejected.connect(dialog.reject)
    root_layout.addWidget(buttonBox)

    def selection_changed(
        selected: QtCore.QItemSelection, deselected: QtCore.QItemSelection
    ) -> None:
        # pylint: disable=no-member
        buttonBox.button(QtWidgets.QDialogButtonBox.StandardButton.Ok).setEnabled(  # type: ignore
            bool(column_list.selectedItems())
        )

    column_list.selectionModel().selectionChanged.connect(selection_changed)  # type: ignore
    if dialog.exec() == QtWidgets.QDialog.Rejected:
        return selected_columns
    return [Column(k.data(QtCore.Qt.UserRole)) for k in column_list.selectedItems()]


class MplCanvas(FigureCanvasQTAgg):
    def __init__(self, parent=None, width=5, height=4, dpi=100):
        fig = Figure(figsize=(width, height), dpi=dpi)
        self.axes = fig.add_subplot(111)
        super().__init__(fig)


class RunTable(QtWidgets.QWidget):
    run_selected = QtCore.pyqtSignal(int)

    def __init__(self, context: Context, tables: Tables, proposal_id: str) -> None:
        super().__init__()

        self._proposal_id = proposal_id
        self._tables = tables

        # Init main widgets
        choose_columns = QtWidgets.QPushButton("Choose columns")
        choose_columns.setIcon(
            self.style().standardIcon(QtWidgets.QStyle.SP_FileDialogDetailedView)
        )
        choose_columns.clicked.connect(self._switch_columns)

        self._context = context
        self._table_view = GeneralTableWidget[Column](
            Column,
            column_headers={c: _column_header(c) for c in Column},
            column_visibility=[
                Column.RUN_ID,
                Column.STATUS,
                Column.SAMPLE,
                Column.REPETITION_RATE,
                Column.TAGS,
            ],
            column_converters={Column.TAGS: _convert_tag_column},
            data_retriever=None,
            parent=self,
        )
        self._table_view.set_menu_callback(self._header_menu_callback)
        self._table_view.row_double_click.connect(self._row_selected)

        log_output = QtWidgets.QPlainTextEdit()
        log_output.setReadOnly(True)

        # Layouting stuff
        root_layout = QtWidgets.QVBoxLayout(self)
        header_layout = QtWidgets.QHBoxLayout()
        header_layout.addWidget(choose_columns, 0, QtCore.Qt.AlignTop)
        header_layout.addWidget(
            QtWidgets.QLabel("Filter query:"), 0, QtCore.Qt.AlignTop
        )
        filter_widget = InfixCompletingLineEdit(self)
        filter_widget.textChanged.connect(self._filter_changed)
        completer = QtWidgets.QCompleter(list(_column_query_names()), self)
        completer.setCompletionMode(QtWidgets.QCompleter.InlineCompletion)
        filter_widget.setCompleter(completer)

        query_layout = QtWidgets.QVBoxLayout()
        # filter_widget = QtWidgets.QLineEdit()
        query_layout.addWidget(filter_widget)
        self._query_error = QtWidgets.QLabel("")
        self._query_error.setStyleSheet("QLabel { font: italic 10px; color: red; }")
        query_layout.addWidget(self._query_error)
        query_layout.setSpacing(0)
        query_layout.setContentsMargins(0, 0, 0, 0)

        header_layout.addLayout(query_layout)

        root_layout.addLayout(header_layout)
        root_layout.addWidget(
            QtWidgets.QLabel(
                "<b>Double-click</b> row to view details. <b>Right-click</b> column header to show options."
            )
        )

        root_layout.addWidget(self._table_view)
        context.db.after_db_created(self._late_init)

    def _header_menu_callback(self, pos: QtCore.QPoint, column: Column) -> None:
        if column in _unplottable_columns:
            return
        menu = QtWidgets.QMenu(self)
        plotAction = menu.addAction(
            self.style().standardIcon(QtWidgets.QStyle.SP_DesktopIcon),
            "Plot this column",
        )
        action = menu.exec_(pos)
        if action == plotAction:
            dialog = QtWidgets.QDialog(self)
            dialog_layout = QtWidgets.QVBoxLayout()
            dialog.setLayout(dialog_layout)

            sc = MplCanvas(self, width=5, height=4, dpi=100)

            df = pd.DataFrame(
                self._table_view.get_column_values(column),
                index=self._table_view.get_column_values(Column.STARTED),
                columns=[_column_header(column)],
            )

            df.plot(ax=sc.axes)

            # Create toolbar, passing canvas as first parament, parent (self, the MainWindow) as second.
            toolbar = NavigationToolbar(sc, self)

            dialog_layout.addWidget(toolbar)
            dialog_layout.addWidget(sc)

            # graph_widget.plot(
            #     [
            #         t.timestamp()
            #         for t in self._table_view.get_column_values(Column.STARTED)
            #     ],
            #     self._table_view.get_column_values(column),
            # )

            # dialog_layout.addWidget(graph_widget)

            dialog.exec()

    def _row_selected(self, row: Dict[Column, Any]) -> None:
        self.run_selected.emit(row[Column.RUN_ID])

    def run_changed(self) -> None:
        logger.info("Refreshing run table")
        self._table_view.refresh()

    def _switch_columns(self) -> None:
        new_columns = _display_column_chooser(self, self._table_view.column_visibility)
        self._table_view.set_column_visibility(new_columns)

    def _late_init(self) -> None:
        logger.info("Late initing")

        self._table_view.set_data_retriever(
            lambda: _retrieve_data_no_connection(self._context.db, self._tables, self._proposal_id)  # type: ignore
        )

    def _filter_changed(self, f: str) -> None:
        try:
            query = parse_query(f, _column_query_names())
            self._table_view.set_filter_query(query)
        except UnexpectedEOF:
            self._query_error.setText("")
        except Exception as e:
            self._query_error.setText(f"{e}")
