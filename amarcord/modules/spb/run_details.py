from dataclasses import dataclass
import datetime
import getpass
from typing import List
from typing import Any
from PyQt5 import QtWidgets
import sqlalchemy as sa
from amarcord.modules.context import Context
from amarcord.qt.tags import Tags
from amarcord.modules.spb.tables import Tables
from amarcord.util import remove_duplicates


@dataclass(frozen=True)
class _RunSimple:
    run_id: int
    finished: bool


@dataclass(frozen=True)
class _Comment:
    author: str
    text: str
    date: datetime.datetime


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
        comments=[
            _Comment(row["author"], row["text"], row["created"])
            for row in run_rows
            if row["author"] is not None
        ],
    )


class RunDetails(QtWidgets.QWidget):
    def __init__(self, context: Context, tables: Tables) -> None:
        super().__init__()

        self._context = context
        with context.db.connect() as conn:
            self._run_ids = _retrieve_run_ids(conn, tables)
            self._sample_ids = _retrieve_sample_ids(conn, tables)
            self._tags = _retrieve_tags(conn, tables)

            if not self._run_ids:
                QtWidgets.QLabel("No runs yet, please wait or create one", self)
            else:
                max_run = max(r.run_id for r in self._run_ids)
                self._run = _retrieve_run(conn, tables, max_run)

                top_row = QtWidgets.QWidget()
                top_layout = QtWidgets.QHBoxLayout()
                top_row.setLayout(top_layout)

                run_selector = QtWidgets.QComboBox()
                run_selector.addItems([str(r.run_id) for r in self._run_ids])
                run_selector.setCurrentText(str(max_run))
                top_layout.addWidget(QtWidgets.QLabel("Run:"))
                top_layout.addWidget(run_selector)
                top_layout.addWidget(QtWidgets.QPushButton("Switch to latest"))
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
                comment_table = QtWidgets.QTableWidget()
                comment_layout.addWidget(comment_table)

                comment_form_layout = QtWidgets.QFormLayout()
                comment_author = QtWidgets.QLineEdit()
                comment_author.setText(getpass.getuser())
                comment_form_layout.addRow(QtWidgets.QLabel("Author"), comment_author)
                comment_input = QtWidgets.QLineEdit()
                comment_input.setClearButtonEnabled(True)
                comment_form_layout.addRow(QtWidgets.QLabel("Text"), comment_input)
                comment_layout.addLayout(comment_form_layout)

                details_column = QtWidgets.QGroupBox("Run Details")
                details_layout = QtWidgets.QFormLayout()
                details_column.setLayout(details_layout)
                sample_chooser = QtWidgets.QComboBox()
                sample_chooser.addItems([str(s) for s in self._sample_ids])
                details_layout.addRow(QtWidgets.QLabel("Sample"), sample_chooser)
                tags = Tags()
                tags.completion(self._tags)
                details_layout.addRow(QtWidgets.QLabel("Tags"), tags)

                root_layout = QtWidgets.QVBoxLayout()
                self.setLayout(root_layout)

                root_layout.addWidget(top_row)

                root_columns = QtWidgets.QHBoxLayout()
                root_layout.addLayout(root_columns)
                root_columns.addWidget(comment_column)
                root_columns.addWidget(details_column)
