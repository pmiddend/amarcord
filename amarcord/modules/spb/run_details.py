from dataclasses import dataclass
import datetime
import getpass
import logging
from typing import List
from typing import Optional
from typing import Any
import humanize
from PyQt5 import QtWidgets
from PyQt5 import QtCore
from PyQt5 import QtGui
import sqlalchemy as sa
from amarcord.modules.context import Context
from amarcord.qt.tags import Tags
from amarcord.modules.spb.tables import Tables
from amarcord.util import remove_duplicates

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _RunSimple:
    run_id: int
    finished: bool


@dataclass(frozen=True)
class _Comment:
    id: int
    author: str
    text: str
    created: datetime.datetime


@dataclass(frozen=True)
class _Run:
    sample_id: int
    tags: List[str]
    status: str
    repetition_rate_mhz: float
    comments: List[_Comment]


def _retrieve_run_ids(conn: Any, tables: Tables) -> List[_RunSimple]:
    return [
        _RunSimple(row[0], row[1] == "finished")
        for row in conn.execute(
            sa.select([tables.run.c.id, tables.run.c.status]).order_by(tables.run.c.id)
        ).fetchall()
    ]


def _retrieve_tags(conn: Any, tables: Tables) -> List[str]:
    return [
        row[0]
        for row in conn.execute(
            sa.select([tables.run_tag.c.tag_text])
            .order_by(tables.run_tag.c.tag_text)
            .distinct()
        ).fetchall()
    ]


def _retrieve_sample_ids(conn: Any, tables: Tables) -> List[int]:
    return [
        row[0]
        for row in conn.execute(
            sa.select([tables.sample.c.sample_id]).order_by(tables.sample.c.sample_id)
        ).fetchall()
    ]


def _change_sample(
    conn: Any, tables: Tables, run_id: int, new_sample: Optional[int]
) -> None:
    conn.execute(
        sa.update(tables.run)
        .where(tables.run.c.id == run_id)
        .values(sample_id=new_sample)
    )


def _change_tags(conn: Any, tables: Tables, run_id: int, new_tags: List[str]) -> None:
    with conn.begin():
        conn.execute(sa.delete(tables.run_tag).where(tables.run_tag.c.run_id == run_id))
        if new_tags:
            conn.execute(
                tables.run_tag.insert(),
                [{"run_id": run_id, "tag_text": t} for t in new_tags],
            )


def _retrieve_run(conn: Any, tables: Tables, run_id: int) -> _Run:
    run_c = tables.run.c
    select_statement = (
        sa.select(
            [
                run_c.id,
                run_c.sample_id,
                run_c.status,
                run_c.repetition_rate_mhz,
                tables.run_tag.c.tag_text,
                tables.run_comment.c.id.label("comment_id"),
                tables.run_comment.c.author,
                tables.run_comment.c.text,
                tables.run_comment.c.created,
            ]
        )
        .select_from(tables.run.outerjoin(tables.run_tag).outerjoin(tables.run_comment))
        .where(run_c.id == run_id)
    )
    run_rows = conn.execute(select_statement).fetchall()
    if not run_rows:
        raise Exception(f"couldn't find any runs with id {run_id}")
    run_meta = run_rows[0]
    return _Run(
        sample_id=run_meta["sample_id"],
        status=run_meta["status"],
        repetition_rate_mhz=run_meta["repetition_rate_mhz"],
        tags=remove_duplicates(
            [row["tag_text"] for row in run_rows if row["tag_text"] is not None]
        ),
        comments=remove_duplicates(
            _Comment(row["comment_id"], row["author"], row["text"], row["created"])
            for row in run_rows
            if row["author"] is not None
        ),
    )


def _add_comment(
    conn: Any, tables: Tables, run_id: int, author: str, text: str
) -> None:
    conn.execute(
        sa.insert(tables.run_comment).values(
            run_id=run_id, author=author, text=text, created=datetime.datetime.utcnow()
        )
    )


def _delete_comment(conn: Any, tables: Tables, comment_id: int) -> None:
    conn.execute(
        sa.delete(tables.run_comment).where(tables.run_comment.c.id == comment_id)
    )


class _CommentTable(QtWidgets.QTableWidget):
    delete_current_row = QtCore.pyqtSignal()

    def contextMenuEvent(self, event: QtGui.QContextMenuEvent) -> None:
        menu = QtWidgets.QMenu(self)
        deleteAction = menu.addAction(
            self.style().standardIcon(QtWidgets.QStyle.SP_DialogCancelButton),
            "Delete comment",
        )
        action = menu.exec_(self.mapToGlobal(event.pos()))
        if action == deleteAction:
            self.delete_current_row.emit()


class RunDetails(QtWidgets.QWidget):
    run_changed = QtCore.pyqtSignal()

    def __init__(self, context: Context, tables: Tables) -> None:
        super().__init__()

        self._context = context
        self._tables = tables
        with context.db.connect() as conn:
            self._run_ids = _retrieve_run_ids(conn, tables)
            self._sample_ids = _retrieve_sample_ids(conn, tables)
            self._tags = _retrieve_tags(conn, tables)

            if not self._run_ids:
                QtWidgets.QLabel("No runs yet, please wait or create one", self)
            else:
                top_row = QtWidgets.QWidget()
                top_layout = QtWidgets.QHBoxLayout()
                top_row.setLayout(top_layout)

                self._run_selector = QtWidgets.QComboBox()
                self._run_selector.addItems([str(r.run_id) for r in self._run_ids])
                self._run_selector.currentTextChanged.connect(
                    lambda run_id_str: self._run_changed(int(run_id_str))
                )
                top_layout.addWidget(QtWidgets.QLabel("Run:"))
                top_layout.addWidget(self._run_selector)
                self._switch_to_latest_button = QtWidgets.QPushButton(
                    "Switch to latest"
                )
                self._switch_to_latest_button.setIcon(
                    self.style().standardIcon(QtWidgets.QStyle.SP_MediaSeekForward)
                )
                top_layout.addWidget(self._switch_to_latest_button)
                top_layout.addWidget(QtWidgets.QCheckBox("Auto switch to latest"))
                top_layout.addItem(
                    QtWidgets.QSpacerItem(
                        40,
                        20,
                        QtWidgets.QSizePolicy.Expanding,
                        QtWidgets.QSizePolicy.Minimum,
                    )
                )

                comment_column = QtWidgets.QGroupBox("Comments")
                comment_layout = QtWidgets.QVBoxLayout()
                comment_column.setLayout(comment_layout)
                self._comment_table = _CommentTable()
                self._comment_table.setSelectionBehavior(
                    # pylint: disable=no-member
                    QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows  # type: ignore
                )
                self._comment_table.delete_current_row.connect(self._delete_comment)

                comment_layout.addWidget(self._comment_table)

                comment_form_layout = QtWidgets.QFormLayout()
                self._comment_author = QtWidgets.QLineEdit()
                self._comment_author.setText(getpass.getuser())
                self._comment_author.textChanged.connect(self._author_changed)
                comment_form_layout.addRow(
                    QtWidgets.QLabel("Author"), self._comment_author
                )
                self._comment_input = QtWidgets.QLineEdit()
                self._comment_input.setClearButtonEnabled(True)
                self._comment_input.textChanged.connect(self._comment_text_changed)
                comment_form_layout.addRow(
                    QtWidgets.QLabel("Text"), self._comment_input
                )
                self._add_comment_button = QtWidgets.QPushButton("Add Comment")
                self._add_comment_button.setIcon(
                    self.style().standardIcon(QtWidgets.QStyle.SP_DialogOkButton)
                )
                self._add_comment_button.setEnabled(False)
                self._add_comment_button.clicked.connect(self._add_comment)
                comment_form_layout.addWidget(self._add_comment_button)
                comment_layout.addLayout(comment_form_layout)

                details_column = QtWidgets.QGroupBox("Run Details")
                details_layout = QtWidgets.QFormLayout()
                details_column.setLayout(details_layout)
                self._sample_chooser = QtWidgets.QComboBox()
                self._sample_chooser.addItems(
                    [str(s) for s in self._sample_ids] + ["None"]
                )
                self._sample_chooser.currentTextChanged.connect(
                    lambda new_sample_str: self._sample_changed(
                        int(new_sample_str) if new_sample_str != "None" else None
                    )
                )
                details_layout.addRow(QtWidgets.QLabel("Sample"), self._sample_chooser)
                self._tags_widget = Tags()
                self._tags_widget.completion(self._tags)
                self._tags_widget.tagsEdited.connect(self._tags_changed)
                details_layout.addRow(QtWidgets.QLabel("Tags"), self._tags_widget)

                root_layout = QtWidgets.QVBoxLayout()
                self.setLayout(root_layout)

                root_layout.addWidget(top_row)

                root_columns = QtWidgets.QHBoxLayout()
                root_layout.addLayout(root_columns)
                root_columns.addWidget(comment_column)
                root_columns.addWidget(details_column)

                self._selected_run = -1
                self._run_changed(max(r.run_id for r in self._run_ids))

    def select_run(self, run_id: int) -> None:
        self._run_changed(run_id)

    def _delete_comment(self) -> None:
        with self._context.db.connect() as conn:
            logger.info("Will delete row %s", self._comment_table.currentRow())
            _delete_comment(
                conn,
                self._tables,
                self._run.comments[self._comment_table.currentRow()].id,
            )
            self._refresh()
            self.run_changed.emit()

    def _tags_changed(self) -> None:
        if self._tags_widget.tagsStr() == self._run.tags:
            return
        with self._context.db.connect() as conn:
            logger.info("Tags have changed!")
            _change_tags(
                conn, self._tables, self._selected_run, self._tags_widget.tagsStr()
            )
            self._tags = _retrieve_tags(conn, self._tables)
            self._tags_widget.completion(self._tags)
            self._refresh()
            logger.info("emitting")
            self.run_changed.emit()

    def _refresh(self) -> None:
        self._run_changed(self._selected_run)

    def _run_changed(self, run_id: int) -> None:
        with self._context.db.connect() as conn:
            self._selected_run = run_id
            self._run = _retrieve_run(conn, self._tables, self._selected_run)
            self._run_selector.setCurrentText(str(self._selected_run))
            self._sample_chooser.setCurrentText(
                str(self._run.sample_id) if self._run.sample_id is not None else "None"
            )
            self._tags_widget.tags(self._run.tags)
            self._comment_table.setColumnCount(3)
            self._comment_table.setRowCount(len(self._run.comments))
            self._comment_table.setHorizontalHeaderLabels(["Created", "Author", "Text"])
            self._comment_table.horizontalHeader().setStretchLastSection(True)
            self._comment_table.verticalHeader().hide()

            now = datetime.datetime.utcnow()
            for row, c in enumerate(self._run.comments):
                self._comment_table.setItem(
                    row,
                    0,
                    QtWidgets.QTableWidgetItem(humanize.naturaltime(now - c.created)),
                )
                self._comment_table.setItem(
                    row, 1, QtWidgets.QTableWidgetItem(c.author)
                )
                self._comment_table.setItem(row, 2, QtWidgets.QTableWidgetItem(c.text))

    def _comment_text_changed(self, new_text: str) -> None:
        self._add_comment_button.setEnabled(
            bool(new_text) and bool(self._comment_author.text())
        )

    def _author_changed(self, new_text: str) -> None:
        self._add_comment_button.setEnabled(
            bool(new_text) and bool(self._comment_input.text())
        )

    def _add_comment(self) -> None:
        with self._context.db.connect() as conn:
            _add_comment(
                conn,
                self._tables,
                self._selected_run,
                self._comment_author.text(),
                self._comment_input.text(),
            )
            self._refresh()
            self._comment_input.setText("")
            self.run_changed.emit()

    def _sample_changed(self, new_sample: Optional[int]) -> None:
        with self._context.db.connect() as conn:
            _change_sample(conn, self._tables, self._selected_run, new_sample)
        self._refresh()
        self.run_changed.emit()
