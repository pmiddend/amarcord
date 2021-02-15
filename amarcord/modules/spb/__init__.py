from typing import Callable
from typing import Optional
from typing import Dict
from typing import Tuple
from typing import Any
from typing import List
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
        editor.tags(index.model().data(index, QtCore.Qt.EditRole))
        editor.completion(index.model().available_tags)

    def setModelData(
        self,
        editor: QtWidgets.QWidget,
        model: QtCore.QAbstractItemModel,
        index: QtCore.QModelIndex,
    ) -> None:
        model.setData(index, set(editor.tagsStr()), QtCore.Qt.EditRole)


class SampleDelegate(QtWidgets.QStyledItemDelegate):
    # pylint: disable=no-self-use
    def createEditor(
        self,
        parent: QtWidgets.QWidget,
        option: QtWidgets.QStyleOptionViewItem,
        index: QtCore.QModelIndex,
    ) -> QtWidgets.QWidget:
        result = QtWidgets.QComboBox(parent)
        logger.info("Available samples: %s", index.model().available_samples)
        result.insertItems(0, [str(s) for s in index.model().available_samples])
        return result

    # pylint: disable=no-self-use
    def setEditorData(
        self, editor: QtWidgets.QWidget, index: QtCore.QModelIndex
    ) -> None:
        editor.setCurrentText(index.data(QtCore.Qt.DisplayRole))

    def setModelData(
        self,
        editor: QtWidgets.QWidget,
        model: QtCore.QAbstractItemModel,
        index: QtCore.QModelIndex,
    ) -> None:
        model.setData(index, editor.currentText(), QtCore.Qt.EditRole)


Row = Dict[Column, Any]


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
                Column.TAGS: set(row[4] for row in rows if row[4] is not None),
            }
        )
    return result


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
            self.available_tags: List[str] = self._get_available_tags(conn)
            self.available_samples: List[str] = self._get_available_samples(conn)

            self._data = _retrieve_data(tables, conn)
            self._filtered_data = self._data
            self._column_to_index = {
                Column.RUN_ID: 0,
                Column.STATUS: 1,
                Column.SAMPLE: 2,
                Column.REPETITION_RATE: 3,
                Column.TAGS: 4,
            }
            self._index_to_column = {v: k for k, v in self._column_to_index.items()}
            self._column_headers = {
                Column.RUN_ID: "Run",
                Column.STATUS: "Status",
                Column.SAMPLE: "Sample",
                Column.REPETITION_RATE: "Repetition Rate",
                Column.TAGS: "Tags",
            }
            self._column_editable = set({Column.TAGS, Column.SAMPLE})
            self._column_setters = {
                Column.TAGS: self._setTags,
                Column.SAMPLE: self._setSample,
            }
            self._column_converters: Dict[Column, Callable[[Any, int], Any]] = {
                Column.TAGS: self._convert_tag_column
            }
            self._column_filters: Dict[Column, str] = {}

    def _row_matches_filters(self, row: Row) -> bool:
        for c, f in self._column_filters.items():
            row_value = row[c]
            if isinstance(row_value, int):
                logger.info(
                    "row value is int, %s and %s: %s",
                    row_value,
                    f,
                    row_value != str_to_int(f),
                )
                if row_value != str_to_int(f):
                    return False
            elif isinstance(row_value, str) and f not in row[c]:
                return False
            else:
                raise Exception("invalid row value type " + str(type(row_value)))
        return True

    def set_filter_query(self, q: Query) -> None:
        self.beginResetModel()
        self._filtered_data = [
            row
            for row in self._data
            if filter_by_query(q, {k.name.lower(): v for k, v in row.items()})
        ]
        self.endResetModel()

    # def set_column_filter(self, c: Column, f: str) -> None:
    #     if f:
    #         self._column_filters[c] = f
    #     else:
    #         self._column_filters.pop(c, None)
    #     self.beginResetModel()
    #     self._filtered_data = [
    #         row for row in self._data if self._row_matches_filters(row)
    #     ]
    #     self.endResetModel()

    def _get_available_tags(self, conn: Any) -> List[str]:
        return [
            t[0]
            for t in conn.execute(
                sa.select([self._tables.table_run_tag.c.tag_text]).distinct()
            ).fetchall()
        ]

    def _get_available_samples(self, conn: Any) -> List[str]:
        return [
            t[0]
            for t in conn.execute(
                sa.select([self._tables.table_sample.c.sample_id])
            ).fetchall()
        ]

    # pylint: disable=no-self-use
    def _convert_tag_column(self, value: Set[str], role: int) -> Any:
        if role == QtCore.Qt.DisplayRole:
            return ", ".join(value)
        if role == QtCore.Qt.EditRole:
            return value
        return QtCore.QVariant()

    def column_index(self, c: Column) -> int:
        return self._column_to_index[c]

    def index_to_column(self, idx: int) -> Column:
        return self._index_to_column[idx]

    def atCell(self, row: int, column: Column) -> Any:
        return self._filtered_data[row][column]

    def setCell(self, row: int, column: Column, value: Any) -> None:
        self._filtered_data[row][column] = value

    def _setSample(self, row: int, value: QtCore.QVariant, role: int) -> bool:
        if not isinstance(value, str):
            raise Exception("didn't get a str in set sample, but " + str(type(value)))

        with self._dbcontext.connect() as conn:
            conn.execute(
                self._tables.table_run.update()
                .where(self._tables.table_run.c.id == self.atCell(row, Column.RUN_ID))
                .values(sample_id=int(value))
            )
        self.setCell(row, Column.SAMPLE, value)
        return True

    def _setTags(self, row: int, value: QtCore.QVariant, role: int) -> bool:
        if not isinstance(value, set):
            raise Exception("didn't get a list in set tags, but " + str(type(value)))

        with self._dbcontext.connect() as conn:
            with conn.begin():
                run_id = self.atCell(row, Column.RUN_ID)
                run_tag = self._tables.table_run_tag
                conn.execute(run_tag.delete().where(run_tag.c.run_id == run_id))
                if value:
                    conn.execute(
                        run_tag.insert(),
                        [{"run_id": run_id, "tag_text": t} for t in value],
                    )
                self.available_tags = self._get_available_tags(conn)

        self.setCell(row, Column.TAGS, value)
        return True

    def rowCount(self, parent: QtCore.QModelIndex = QtCore.QModelIndex()) -> int:
        return len(self._filtered_data)

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
        v = self._filtered_data[index.row()][self._index_to_column[index.column()]]
        column_converter = self._column_converters.get(
            self._index_to_column[index.column()], None
        )
        if column_converter is not None:
            return column_converter(v, role)
        if role in (QtCore.Qt.DisplayRole, QtCore.Qt.EditRole):
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
        self._table_view = QtWidgets.QTableView(self)
        # self._table_view = FilterHeaderTableView(self)
        self._table_view.setAlternatingRowColors(True)
        self._delegates: List[QtWidgets.QAbstractItemDelegate] = []
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
        header_layout.addWidget(choose_columns, 0, QtCore.Qt.AlignTop)
        header_layout.addWidget(open_inspector, 0, QtCore.Qt.AlignTop)
        header_layout.addWidget(
            QtWidgets.QLabel("Filter query:"), 0, QtCore.Qt.AlignTop
        )
        # filter_widget = InfixCompletingLineEdit(self)
        # completer = QtWidgets.QCompleter(["fooquxbar", "bar"], self)
        # completer.setCompletionMode(QtWidgets.QCompleter.InlineCompletion)
        # filter_widget.setCompleter(completer)

        query_layout = QtWidgets.QVBoxLayout()
        filter_widget = QtWidgets.QLineEdit()
        filter_widget.textChanged.connect(self._filter_changed)
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
        tags_delegate = TagsDelegate()
        self._table_view.setItemDelegateForColumn(
            self._table_model.column_index(Column.TAGS), tags_delegate
        )
        sample_delegate = SampleDelegate()
        self._table_view.setItemDelegateForColumn(
            self._table_model.column_index(Column.SAMPLE), sample_delegate
        )
        # The table doesn't own the delegates, thus our storage here
        self._delegates.extend([tags_delegate, sample_delegate])

        # editor = QtWidgets.QWidget()
        # edlay = QtWidgets.QVBoxLayout()
        # edlay.addWidget(QtWidgets.QLabel("Run ID WWWWWWWWWWWW"))
        # editorLine = QtWidgets.QLineEdit()
        # editorLine.setClearButtonEnabled(True)
        # editorLine.setPlaceholderText("Filter")
        # # editorLine.returnPressed.connect(self.filterActivated.emit)
        # # editorLine.textChanged.connect(partial(self._textChanged, k))
        # edlay.addWidget(editorLine)
        # editor.setLayout(edlay)

        # self._table_view.setHorizontalHeaderWidget(0, editor)
        # self._table_view.setHorizontalHeaderWidget(0, QtWidgets.QLineEdit())

        # header = FilterHeader(self._table_view)
        # header.setColumnFilters(
        #     {self._table_model.column_index(Column.RUN_ID): Filter()}
        # )
        # header.filterChanged.connect(self._filter_changed)
        # self._table_view.setHorizontalHeader(header)

    def _filter_changed(self, f: str) -> None:
        if self._table_model is None:
            return
        try:
            query = parse_query(f, {f.name.lower() for f in Column})
            self._table_model.set_filter_query(query)
        except UnexpectedEOF:
            self._query_error.setText("")
        except Exception as e:
            traceback.print_exc()
            self._query_error.setText(f"{e}")


# header.filterActivated.connect(self.handleFilterActivated)

# def handleFilterActivated(self):
#     header = self.view.horizontalHeader()
#     for index in range(header.count()):
#         if index != 4:
#             print(index, header.filterText(index))
#         else:
#             print("Button")
