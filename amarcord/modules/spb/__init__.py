from typing import Callable
from typing import Optional
from typing import Dict
from typing import Tuple
from typing import Any
from typing import List
from typing import Set
from typing import Union
from dataclasses import dataclass
from itertools import groupby
import datetime
import time
import logging
from enum import Enum, auto
import pandas as pd
import sqlalchemy as sa
from PyQt5 import QtWidgets
from PyQt5 import QtCore
from amarcord.modules.context import Context
from amarcord.modules.dbcontext import DBContext
from amarcord.qt.filter_header import FilterHeader
from amarcord.qt.logging_handler import QtLoggingHandler
from amarcord.qt.tags import Tags

logger = logging.getLogger(__name__)


class Column(Enum):
    RUN_ID = auto()
    STATUS = auto()
    REPETITION_RATE = auto()
    TAGS = auto()


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
class _RunTables:
    table_sample: sa.Table
    table_run_tag: sa.Table
    table_run: sa.Table


_db_column_to_readable: Dict[str, str] = {}


class TagsDelegate(QtWidgets.QStyledItemDelegate):
    # pylint: disable=no-self-use
    def createEditor(
        self,
        parent: QtWidgets.QWidget,
        option: QtWidgets.QStyleOptionViewItem,
        index: QtCore.QModelIndex,
    ) -> QtWidgets.QWidget:
        return Tags(parent)

    # pylint: disable=no-self-use
    def setEditorData(
        self, editor: QtWidgets.QWidget, index: QtCore.QModelIndex
    ) -> None:
        logger.info(
            "Editor data will be %s", index.model().data(index, QtCore.Qt.EditRole)
        )
        editor.tags(index.model().data(index, QtCore.Qt.EditRole))
        editor.completion(index.model().available_tags)

    def setModelData(
        self,
        editor: QtWidgets.QWidget,
        model: QtCore.QAbstractItemModel,
        index: QtCore.QModelIndex,
    ) -> None:
        model.setData(index, set(editor.tagsStr()), QtCore.Qt.EditRole)


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

            self.available_tags: List[str] = self._get_available_tags(conn)

            select_stmt = (
                sa.select(
                    [run.c.id, run.c.status, run.c.repetition_rate_mhz, tag.c.tag_text]
                )
                .select_from(run.outerjoin(tag))
                .order_by(run.c.id)
            )
            self._data: List[List[Any]] = []
            for _run_id, run_rows in groupby(
                conn.execute(select_stmt).fetchall(), lambda x: x[0]
            ):
                rows: List[Tuple[str, float, str, str]] = list(run_rows)
                first_row = rows[0]
                self._data.append(
                    [
                        first_row[0],
                        first_row[1],
                        first_row[2],
                        set(row[3] for row in rows if row[3] is not None),
                    ]
                )
            self._column_to_index = {
                Column.RUN_ID: 0,
                Column.STATUS: 1,
                Column.REPETITION_RATE: 2,
                Column.TAGS: 3,
            }
            self._index_to_column = {v: k for k, v in self._column_to_index.items()}
            self._column_headers = {
                Column.RUN_ID: "Run",
                Column.STATUS: "Status",
                Column.REPETITION_RATE: "Repetition Rate",
                Column.TAGS: "Tags",
            }
            self._column_editable = set({Column.TAGS})
            self._column_setters = {Column.TAGS: self._setTags}
            self._column_converters: Dict[Column, Callable[[Any, int], Any]] = {
                Column.TAGS: self._convert_tag_column
            }

    def _get_available_tags(self, conn: Any) -> List[str]:
        return [
            t[0]
            for t in conn.execute(
                sa.select([self._tables.table_run_tag.c.tag_text]).distinct()
            ).fetchall()
        ]

    # pylint: disable=no-self-use
    def _convert_tag_column(self, value: Set[str], role: int) -> Any:
        if role == QtCore.Qt.DisplayRole:
            return ", ".join(value)
        if role == QtCore.Qt.EditRole:
            return value
        return QtCore.QVariant()

    def tags_column_index(self) -> int:
        return self._column_to_index[Column.TAGS]

    def _setTags(self, row: int, value: QtCore.QVariant, role: int) -> bool:
        if not isinstance(value, set):
            raise Exception("didn't get a list in set tags, but " + str(type(value)))

        logger.info("Setting tags to %s", value)
        with self._dbcontext.connect() as conn:
            with conn.begin():
                run_id = self._data[row][self._column_to_index[Column.RUN_ID]]
                run_tag = self._tables.table_run_tag
                conn.execute(run_tag.delete().where(run_tag.c.run_id == run_id))
                if value:
                    conn.execute(
                        run_tag.insert(),
                        [{"run_id": run_id, "tag_text": t} for t in value],
                    )
                self.available_tags = self._get_available_tags(conn)

        self._data[row][self._column_to_index[Column.TAGS]] = value
        return True

    def rowCount(self, parent: QtCore.QModelIndex = QtCore.QModelIndex()) -> int:
        return len(self._data)

    def columnCount(self, parent: QtCore.QModelIndex = QtCore.QModelIndex()) -> int:
        return len(Column)

    def flags(self, index: QtCore.QModelIndex) -> QtCore.Qt.ItemFlags:
        # Taken from the tutorial:
        # https://doc.qt.io/qt-5/model-view-programming.html#creating-new-models
        if not index.isValid():
            return QtCore.Qt.ItemIsEnabled  # type: ignore

        result = super().flags(index)
        if self._index_to_column[index.column()] in self._column_editable:
            result |= QtCore.Qt.ItemIsEditable  # type: ignore
        return result

    def setData(
        self,
        index: QtCore.QModelIndex,
        value: QtCore.QVariant,
        role: int = QtCore.Qt.EditRole,
    ) -> bool:
        result = self._column_setters[self._index_to_column[index.column()]](
            index.row(), value, role
        )
        if result:
            self.dataChanged.emit(index, index, [role])
        return result

    def headerData(
        self,
        section: int,
        orientation: QtCore.Qt.Orientation,
        role: int = QtCore.Qt.DisplayRole,
    ) -> QtCore.QVariant:
        if orientation == QtCore.Qt.Vertical or role != QtCore.Qt.DisplayRole:
            return QtCore.QVariant()
        return QtCore.QVariant(self._column_headers[self._index_to_column[section]])

    def data(
        self,
        index: QtCore.QModelIndex,
        role: int = QtCore.Qt.DisplayRole,
    ) -> Any:
        v = self._data[index.row()][index.column()]
        column_converter = self._column_converters.get(
            self._index_to_column[index.column()], None
        )
        if column_converter is not None:
            return column_converter(v, role)
        if role == QtCore.Qt.DisplayRole:
            return str(v)
        return QtCore.QVariant()


class RunTable(QtWidgets.QWidget):
    def __init__(self, context: Context) -> None:
        super().__init__()

        # Init main widgets
        choose_columns = QtWidgets.QPushButton("Choose columns")
        open_inspector = QtWidgets.QPushButton("Open inspector")

        self._table_model: Optional[DBModel] = None
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

        root_layout.addLayout(header_layout)

        root_layout.addWidget(self._table_view)

        root_layout.addWidget(QtWidgets.QLabel("Log:"))
        root_layout.addWidget(log_output)
        logger.addHandler(QtLoggingHandler(log_output))

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
                )
            )

        self._table_model = DBModel(self._context.db, self._tables)
        self._table_view.setModel(self._table_model)
        self._table_view.verticalHeader().hide()
        self._table_view.horizontalHeader().setStretchLastSection(True)
        self._table_view.setItemDelegateForColumn(
            self._table_model.tags_column_index(), TagsDelegate()
        )

    # def handleFilterActivated(self):
    #     header = self.view.horizontalHeader()
    #     for index in range(header.count()):
    #         if index != 4:
    #             print(index, header.filterText(index))
    #         else:
    #             print("Button")
