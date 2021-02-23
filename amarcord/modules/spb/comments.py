import datetime
import getpass
import logging
from dataclasses import replace
from enum import Enum
from typing import Dict, Final, List, Optional

import humanize
from PyQt5 import QtCore, QtGui, QtWidgets

from amarcord.modules.spb.queries import Comment, SPBQueries

logger = logging.getLogger(__name__)


class Column(Enum):
    CREATED = 0
    AUTHOR = 1
    TEXT = 2


_column_labels: Final[Dict[Column, str]] = {
    Column.CREATED: "Created",
    Column.AUTHOR: "Author",
    Column.TEXT: "Text",
}


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


def _date_cell(
    now: datetime.datetime, created: datetime.datetime
) -> QtWidgets.QTableWidgetItem:
    date_cell = QtWidgets.QTableWidgetItem(humanize.naturaltime(now - created))
    date_cell.setFlags(QtCore.Qt.ItemIsSelectable)
    return date_cell


class Comments(QtWidgets.QWidget):
    comments_changed = QtCore.pyqtSignal()

    def __init__(
        self, db: SPBQueries, parent: Optional[QtWidgets.QWidget] = None
    ) -> None:
        super().__init__(parent)

        self._db = db
        layout = QtWidgets.QVBoxLayout()
        self.setLayout(layout)

        self._comment_table = _CommentTable()
        self._comment_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self._comment_table.delete_current_row.connect(self._slot_delete_comment)
        self._comment_table.cellChanged.connect(self._comment_cell_changed)

        layout.addWidget(self._comment_table)

        comment_form_layout = QtWidgets.QFormLayout()
        self._comment_author = QtWidgets.QLineEdit()
        self._comment_author.setText(getpass.getuser())
        self._comment_author.textChanged.connect(self._slot_author_changed)
        comment_form_layout.addRow("Author", self._comment_author)
        self._comment_input = QtWidgets.QLineEdit()
        self._comment_input.setClearButtonEnabled(True)
        self._comment_input.textChanged.connect(self._slot_comment_text_changed)
        self._comment_input.returnPressed.connect(self._slot_add_comment)
        comment_form_layout.addRow("Text", self._comment_input)
        self._add_comment_button = QtWidgets.QPushButton("Add Comment")
        self._add_comment_button.setIcon(
            self.style().standardIcon(QtWidgets.QStyle.SP_DialogOkButton)
        )
        self._add_comment_button.setEnabled(False)
        self._add_comment_button.clicked.connect(self._slot_add_comment)
        comment_form_layout.addWidget(self._add_comment_button)
        layout.addLayout(comment_form_layout)

        self._comments: List[Comment] = []
        self._run_id: Optional[int] = None
        self._timer = QtCore.QTimer(self)
        self._timer.setInterval(10 * 1000)
        self._timer.timeout.connect(self._refresh_times)
        self._timer.start()

    def set_comments(self, run_id: int, comments: List[Comment]) -> None:
        assert all(c.id is not None for c in comments)

        self._run_id = run_id
        self._comments = comments

        self._comment_table.setColumnCount(3)
        self._comment_table.setRowCount(len(self._comments))
        self._comment_table.setHorizontalHeaderLabels(
            [_column_labels[c] for c in Column]
        )
        self._comment_table.horizontalHeader().setStretchLastSection(True)
        self._comment_table.verticalHeader().hide()

        now = datetime.datetime.utcnow()
        for row, c in enumerate(self._comments):
            self._set_row(c, now, row)

    def _set_row(self, c: Comment, now: datetime.datetime, row: int) -> None:
        self._comment_table.setItem(
            row,
            Column.CREATED.value,
            _date_cell(now, c.created),
        )
        self._comment_table.setItem(
            row, Column.AUTHOR.value, QtWidgets.QTableWidgetItem(c.author)
        )
        self._comment_table.setItem(
            row, Column.TEXT.value, QtWidgets.QTableWidgetItem(c.text)
        )

    def _slot_delete_comment(self) -> None:
        with self._db.connect() as conn:
            row_idx = self._comment_table.currentRow()
            comment = self._comments[row_idx]
            assert comment.id is not None
            self._db.delete_comment(conn, comment.id)
            self._comments.pop(row_idx)
            self._comment_table.removeRow(row_idx)
            self.comments_changed.emit()

    def _comment_cell_changed(self, row: int, column: int) -> None:
        with self._db.connect() as conn:
            i = self._comment_table.item(row, column).text()
            c = self._comments[row]
            c = replace(
                c,
                author=i if column == Column.AUTHOR.value else c.author,
                text=i if column == Column.TEXT.value else c.text,
            )
            self._db.change_comment(conn, c)
            self.comments_changed.emit()

    def _slot_comment_text_changed(self, new_text: str) -> None:
        self._add_comment_button.setEnabled(
            bool(new_text) and bool(self._comment_author.text())
        )

    def _slot_author_changed(self, new_text: str) -> None:
        self._add_comment_button.setEnabled(
            bool(new_text) and bool(self._comment_input.text())
        )

    def _slot_add_comment(self) -> None:
        if not self._comment_author.text() or not self._comment_input.text():
            return
        assert self._run_id is not None
        with self._db.connect() as conn:
            now = datetime.datetime.utcnow()
            comment_id = self._db.add_comment(
                conn,
                self._run_id,
                self._comment_author.text(),
                self._comment_input.text(),
            )
            new_comment = Comment(
                comment_id, self._comment_author.text(), self._comment_input.text(), now
            )
            self._comments.append(new_comment)
            self._comment_table.setRowCount(self._comment_table.rowCount() + 1)
            self._comment_table.blockSignals(True)
            self._set_row(
                new_comment, datetime.datetime.utcnow(), len(self._comments) - 1
            )
            self._comment_table.blockSignals(False)
            self._comment_input.setText("")
            self.comments_changed.emit()
            self._refresh_times()

    def _refresh_times(self) -> None:
        self._comment_table.blockSignals(True)
        for row, c in enumerate(self._comments):
            self._comment_table.setItem(
                row,
                Column.CREATED.value,
                _date_cell(datetime.datetime.utcnow(), c.created),
            )
        self._comment_table.blockSignals(False)
