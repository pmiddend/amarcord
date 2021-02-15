from typing import Callable
from typing import TypeVar
from typing import Optional
from typing import Dict
from typing import Tuple
from typing import Any
from typing import List
from typing import Iterable
from typing import Set
from typing import Union
import traceback
from dataclasses import dataclass
from itertools import groupby
from functools import partial
import datetime
import time
import logging
from enum import Enum, auto
import pandas as pd
import sqlalchemy as sa
from PyQt5 import QtWidgets
from PyQt5 import QtCore
from amarcord.util import str_to_int
from amarcord.modules.context import Context
from amarcord.modules.dbcontext import DBContext
from amarcord.qt.alternative_filter_header import FilterHeader, FilterHeaderTableView
from amarcord.qt.logging_handler import QtLoggingHandler
from amarcord.qt.infix_completer import InfixCompletingLineEdit
from amarcord.qt.table import GeneralTableWidget
from amarcord.qt.tags import Tags
from amarcord.query_parser import parse_query
from amarcord.query_parser import filter_by_query
from amarcord.query_parser import Query
from amarcord.query_parser import UnexpectedEOF

logger = logging.getLogger(__name__)


class Column(Enum):
    RUN_ID = auto()
    STATUS = auto()
    SAMPLE = auto()
    REPETITION_RATE = auto()
    TAGS = auto()
    PULSE_ENERGY = auto()


def _column_query_names() -> Set[str]:
    return {f.name.lower() for f in Column}


def table_sample(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "Sample",
        metadata,
        sa.Column("sample_id", sa.Integer, primary_key=True),
        sa.Column("sample_name", sa.String(length=255)),
    )


def table_run_tag(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "RunTag",
        metadata,
        sa.Column("run_id", sa.Integer, sa.ForeignKey("Run.id")),
        sa.Column("tag_text", sa.String(length=255), nullable=False),
    )


def table_run(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "Run",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("modified", sa.DateTime, nullable=False),
        sa.Column("status", sa.String(length=255), nullable=False),
        sa.Column("sample_id", sa.Integer, nullable=False),
        sa.Column("repetition_rate_mhz", sa.Float, nullable=False),
        sa.Column("pulse_energy_mj", sa.Float, nullable=False),
        # sa.Column("pulses_per_train", sa.Integer, nullable=False),
        # sa.Column("xray_energy_kev", sa.Float, nullable=False),
        # sa.Column("injector_position_z", sa.Float, nullable=False),
    )


@dataclass(frozen=True)
class _RunTables:
    table_sample: sa.Table
    table_run_tag: sa.Table
    table_run: sa.Table


Row = Dict[Column, Any]


def _retrieve_data_no_connection(context: DBContext, tables: _RunTables) -> List[Row]:
    with context.connect() as conn:
        return _retrieve_data(tables, conn)


def _retrieve_data(tables: _RunTables, conn: Any) -> List[Row]:
    run = tables.table_run
    tag = tables.table_run_tag

    select_stmt = (
        sa.select(
            [
                run.c.id,
                run.c.status,
                run.c.sample_id,
                run.c.repetition_rate_mhz,
                run.c.pulse_energy_mj,
                tag.c.tag_text,
            ]
        )
        .select_from(run.outerjoin(tag))
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
    }
    return d[c]


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
            QtCore.QItemSelectionModel.SelectionFlag.Select,
        )
    root_layout.addWidget(column_list)
    buttonBox = QtWidgets.QDialogButtonBox(
        QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel
    )
    buttonBox.accepted.connect(dialog.accept)
    buttonBox.rejected.connect(dialog.reject)
    root_layout.addWidget(buttonBox)

    def selection_changed(
        selected: QtCore.QItemSelection, deselected: QtCore.QItemSelection
    ) -> None:
        buttonBox.button(QtWidgets.QDialogButtonBox.StandardButton.Ok).setEnabled(
            bool(column_list.selectedItems())
        )

    column_list.selectionModel().selectionChanged.connect(selection_changed)
    if dialog.exec() == QtWidgets.QDialog.Rejected:
        return selected_columns
    return [Column(k.data(QtCore.Qt.UserRole)) for k in column_list.selectedItems()]


class RunTable(QtWidgets.QWidget):
    def __init__(self, context: Context) -> None:
        super().__init__()

        # Init main widgets
        choose_columns = QtWidgets.QPushButton("Choose columns")
        choose_columns.setIcon(
            self.style().standardIcon(QtWidgets.QStyle.SP_FileDialogDetailedView)
        )
        choose_columns.clicked.connect(self._switch_columns)
        open_inspector = QtWidgets.QPushButton("Open inspector")

        self._context = context
        self._tables = _RunTables(
            table_sample(context.db.metadata),
            table_run_tag(context.db.metadata),
            table_run(context.db.metadata),
        )
        context.db.after_db_created(self._late_init)
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

        log_output = QtWidgets.QPlainTextEdit()
        log_output.setReadOnly(True)

        # Layouting stuff
        root_layout = QtWidgets.QVBoxLayout(self)
        header_layout = QtWidgets.QHBoxLayout()
        header_layout.addWidget(choose_columns, 0, QtCore.Qt.AlignTop)
        header_layout.addWidget(open_inspector, 0, QtCore.Qt.AlignTop)
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

        root_layout.addWidget(self._table_view)

        root_layout.addWidget(QtWidgets.QLabel("Log:"))
        root_layout.addWidget(log_output)
        logger.addHandler(QtLoggingHandler(log_output))

    def _switch_columns(self) -> None:
        new_columns = _display_column_chooser(self, self._table_view.column_visibility)
        self._table_view.set_column_visibility(new_columns)

    def _late_init(self) -> None:
        logger.info("Late initing")

        with self._context.db.connect() as conn:
            first_sample_result = conn.execute(
                self._tables.table_sample.insert().values(sample_name="first sample")
            )
            conn.execute(
                self._tables.table_sample.insert().values(sample_name="second sample")
            )
            first_sample_id = first_sample_result.inserted_primary_key[0]

            run_id = conn.execute(
                self._tables.table_run.insert().values(
                    modified=datetime.datetime.now(),
                    status="finished",
                    sample_id=first_sample_id,
                    repetition_rate_mhz=3.5,
                    pulse_energy_mj=1,
                )
            ).inserted_primary_key[0]

            conn.execute(
                self._tables.table_run_tag.insert().values(run_id=run_id, tag_text="t1")
            )
            conn.execute(
                self._tables.table_run_tag.insert().values(run_id=run_id, tag_text="t2")
            )

            conn.execute(
                self._tables.table_run.insert().values(
                    modified=datetime.datetime.now(),
                    status="running",
                    sample_id=first_sample_id,
                    repetition_rate_mhz=4.3,
                    pulse_energy_mj=2,
                )
            )

        self._table_view.set_data_retriever(
            lambda: _retrieve_data_no_connection(self._context.db, self._tables)
        )

    def _filter_changed(self, f: str) -> None:
        try:
            query = parse_query(f, _column_query_names())
            self._table_view.set_filter_query(query)
        except UnexpectedEOF:
            self._query_error.setText("")
        except Exception as e:
            self._query_error.setText(f"{e}")
