from typing import Callable
from typing import Optional
from typing import Dict
from typing import Tuple
from typing import Any
from typing import List
from typing import Union
from dataclasses import dataclass
from itertools import groupby
import datetime
import time
import logging
import pandas as pd
from pint import Quantity, UnitRegistry
import sqlalchemy as sa
from PyQt5 import QtWidgets
from PyQt5 import QtCore
from amarcord.modules.context import Context
from amarcord.modules.dbcontext import DBContext
from amarcord.qt.filter_header import FilterHeader
from amarcord.qt.logging_handler import QtLoggingHandler
from amarcord.qt.tags import Tags

logger = logging.getLogger(__name__)

ureg = UnitRegistry()


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
        # sa.Column("pulse_energy_mj", sa.Float, nullable=False),
        # sa.Column("pulses_per_train", sa.Integer, nullable=False),
        # sa.Column("xray_energy_kev", sa.Float, nullable=False),
        # sa.Column("injector_position_z", sa.Float, nullable=False),
    )


@dataclass(frozen=True)
class Run:
    run_id: int
    repetition_rate: Quantity


@dataclass(frozen=True)
class _RunTables:
    table_sample: sa.Table
    table_run_tag: sa.Table
    table_run: sa.Table


_db_column_to_readable: Dict[str, str] = {}


class DBModel(QtCore.QAbstractTableModel):
    def __init__(
        self,
        dbcontext: DBContext,
        tables: _RunTables,
        parent: Optional[QtCore.QObject] = None,
    ) -> None:
        super().__init__(parent)

        self._tables = tables
        self._dbcontext = dbcontext

        with self._dbcontext.connect() as conn:
            run = self._tables.table_run
            tag = self._tables.table_run_tag
            select_stmt = (
                sa.select(
                    [run.c.id, run.c.status, run.c.repetition_rate_mhz, tag.c.tag_text]
                )
                .select_from(run.outerjoin(tag))
                .order_by(run.c.id)
            )
            self._data: List[Tuple[str, float, str, List[str]]] = []
            for _run_id, run_rows in groupby(
                conn.execute(select_stmt).fetchall(), lambda x: x[0]
            ):
                rows = list(run_rows)
                first_row = rows[0]  # type: ignore
                self._data.append(
                    (
                        first_row[0],
                        first_row[1],
                        first_row[2],
                        [row[3] for row in rows if row[3] is not None],
                    )
                )
            self._column_headers = ("Run", "Status", "Repetition Rate", "Tags")
            self._column_converters = (None, None, None, lambda x: ", ".join(x))

    def rowCount(self, parent: QtCore.QModelIndex = QtCore.QModelIndex()) -> int:
        return len(self._data)

    def columnCount(self, parent: QtCore.QModelIndex = QtCore.QModelIndex()) -> int:
        return len(self._column_headers)

    def headerData(
        self,
        section: int,
        orientation: QtCore.Qt.Orientation,
        role: int = QtCore.Qt.DisplayRole,
    ) -> QtCore.QVariant:
        if orientation == QtCore.Qt.Vertical or role != QtCore.Qt.DisplayRole:
            return QtCore.QVariant()
        return QtCore.QVariant(self._column_headers[section])

    def data(
        self,
        index: QtCore.QModelIndex,
        role: int = QtCore.Qt.DisplayRole,
    ) -> QtCore.QVariant:
        if role == QtCore.Qt.DisplayRole:
            v = self._data[index.row()][index.column()]
            column_converter = self._column_converters[index.column()]
            if column_converter is not None:
                v = column_converter(v)
            else:
                v = str(v)
            return QtCore.QVariant(v)
        return QtCore.QVariant()


class MockModel(QtCore.QAbstractTableModel):
    def __init__(self, parent: Optional[QtCore.QObject] = None) -> None:
        super().__init__(parent)

    def rowCount(self, parent: QtCore.QModelIndex = QtCore.QModelIndex()) -> int:
        return 2

    def columnCount(self, parent: QtCore.QModelIndex = QtCore.QModelIndex()) -> int:
        return 3

    def data(
        self,
        index: QtCore.QModelIndex,
        role: int = QtCore.Qt.DisplayRole,
    ) -> QtCore.QVariant:
        if role == QtCore.Qt.DisplayRole:
            a = [
                ["row 1 col 1", "row 1 col 2", "row 1 col 3"],
                ["row 2 col 1", "row 2 col 2", "row 2 col 3"],
            ]
            return QtCore.QVariant(a[index.row()][index.column()])
        return QtCore.QVariant()


class RunTable(QtWidgets.QWidget):
    def __init__(self, context: Context) -> None:
        super().__init__()

        # Init main widgets
        choose_columns = QtWidgets.QPushButton("Choose columns")
        open_inspector = QtWidgets.QPushButton("Open inspector")

        self._context = context
        self._table_view = QtWidgets.QTableView()
        self._table_view.setAlternatingRowColors(True)
        # header = FilterHeader(self._table_view)
        # header.setFilterBoxes(self._table_model.columnCount())
        # header.filterActivated.connect(self.handleFilterActivated)
        # self._table_view.setHorizontalHeader(header)
        self._tables = _RunTables(
            table_sample(context.db.metadata),
            table_run_tag(context.db.metadata),
            table_run(context.db.metadata),
        )
        context.db.after_db_created(self._late_init)

        log_output = QtWidgets.QPlainTextEdit()
        log_output.setReadOnly(True)

        # Layouting stuff
        root_layout = QtWidgets.QVBoxLayout(self)
        header_layout = QtWidgets.QHBoxLayout()
        header_layout.addWidget(choose_columns)
        header_layout.addWidget(open_inspector)

        tags = Tags(self)
        tags.completion(["foo", "bar"])
        header_layout.addWidget(tags)
        root_layout.addLayout(header_layout)

        root_layout.addWidget(self._table_view)

        root_layout.addWidget(QtWidgets.QLabel("Log:"))
        root_layout.addWidget(log_output)
        logger.addHandler(QtLoggingHandler(log_output))

    def _late_init(self) -> None:
        logger.info("Late initing")

        with self._context.db.connect() as conn:
            sample_result = conn.execute(
                self._tables.table_sample.insert().values(sample_name="first sample")
            )
            sample_id = sample_result.inserted_primary_key[0]

            run_id = conn.execute(
                self._tables.table_run.insert().values(
                    modified=datetime.datetime.now(),
                    status="finished",
                    sample_id=sample_id,
                    repetition_rate_mhz=3.5,
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
                    sample_id=sample_id,
                    repetition_rate_mhz=4.3,
                )
            )

        self._table_view.setModel(DBModel(self._context.db, self._tables))
        self._table_view.verticalHeader().hide()
        self._table_view.horizontalHeader().setStretchLastSection(True)

    # def handleFilterActivated(self):
    #     header = self.view.horizontalHeader()
    #     for index in range(header.count()):
    #         if index != 4:
    #             print(index, header.filterText(index))
    #         else:
    #             print("Button")
