from dataclasses import replace
import datetime
import getpass
import logging
from typing import Optional
import humanize
from PyQt5 import QtWidgets
from PyQt5 import QtCore
from PyQt5 import QtGui
from amarcord.modules.spb.new_run_dialog import new_run_dialog
from amarcord.modules.context import Context
from amarcord.qt.tags import Tags
from amarcord.modules.spb.tables import Tables
from amarcord.modules.spb.run_id import RunId
from amarcord.modules.spb.proposal_id import ProposalId
from amarcord.modules.spb.queries import SPBQueries

logger = logging.getLogger(__name__)


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

    def __init__(
        self, context: Context, tables: Tables, proposal_id: ProposalId
    ) -> None:
        super().__init__()

        self._proposal_id = proposal_id
        self._context = context
        self._db = SPBQueries(context.db, tables)
        with context.db.connect() as conn:
            self._run_ids = self._db.retrieve_run_ids(conn, self._proposal_id)
            self._sample_ids = self._db.retrieve_sample_ids(conn)
            self._tags = self._db.retrieve_tags(conn)

            if not self._run_ids:
                QtWidgets.QLabel("No runs yet, please wait or create one", self)
            else:
                top_row = QtWidgets.QWidget()
                top_layout = QtWidgets.QHBoxLayout()
                top_row.setLayout(top_layout)

                self._run_selector = QtWidgets.QComboBox()
                self._run_selector.addItems([str(r.run_id) for r in self._run_ids])
                self._run_selector.currentTextChanged.connect(self._current_run_changed)
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
                manual_creation = QtWidgets.QPushButton("New Run")
                manual_creation.setIcon(
                    self.style().standardIcon(QtWidgets.QStyle.SP_FileDialogNewFolder)
                )
                manual_creation.clicked.connect(self._new_run)
                top_layout.addWidget(manual_creation)

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

                self._selected_run = RunId(-1)
                self._run_changed(max(r.run_id for r in self._run_ids))

    def _current_run_changed(self, new_run_id: str) -> None:
        self._run_changed(RunId(int(new_run_id)))

    def _new_run(self) -> None:
        new_run_id = new_run_dialog(
            parent=self,
            proposal_id=self._proposal_id,
            queries=self._db,
        )
        if new_run_id is not None:
            logger.info("Selecting new run %s", new_run_id)
            self.select_run(new_run_id)
            self.run_changed.emit()

    def select_run(self, run_id: RunId) -> None:
        self._run_changed(run_id)

    def _delete_comment(self) -> None:
        with self._context.db.connect() as conn:
            logger.info("Will delete row %s", self._comment_table.currentRow())
            self._db.delete_comment(
                conn,
                self._run.comments[self._comment_table.currentRow()].id,
            )
            self._refresh()
            self.run_changed.emit()

    def _tags_changed(self) -> None:
        if self._tags_widget.tagsStr() == self._run.tags:
            return
        with self._context.db.connect() as conn:
            logger.info("Tags have changed!")
            self._db.change_tags(conn, self._selected_run, self._tags_widget.tagsStr())
            self._tags = self._db.retrieve_tags(conn)
            self._tags_widget.completion(self._tags)
            self._refresh()
            self.run_changed.emit()

    def _refresh(self) -> None:
        self._run_changed(self._selected_run)

    def _run_changed(self, run_id: RunId) -> None:
        with self._context.db.connect() as conn:
            self._selected_run = run_id
            self._run = self._db.retrieve_run(conn, self._selected_run)

            new_run_ids = self._db.retrieve_run_ids(conn, self._proposal_id)

            if new_run_ids != self._run_ids:
                self._run_ids = new_run_ids
                self._run_selector.blockSignals(True)
                self._run_selector.clear()
                self._run_selector.addItems([str(r.run_id) for r in self._run_ids])

            self._run_selector.setCurrentText(str(self._selected_run))
            self._run_selector.blockSignals(False)
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
                date_cell = QtWidgets.QTableWidgetItem(
                    humanize.naturaltime(now - c.created)
                )
                date_cell.setFlags(QtCore.Qt.ItemIsSelectable)
                self._comment_table.setItem(
                    row,
                    0,
                    date_cell,
                )
                self._comment_table.setItem(
                    row, 1, QtWidgets.QTableWidgetItem(c.author)
                )
                self._comment_table.setItem(row, 2, QtWidgets.QTableWidgetItem(c.text))
            self._comment_table.cellChanged.connect(self._comment_cell_changed)

    def _comment_cell_changed(self, row: int, column: int) -> None:
        with self._context.db.connect() as conn:
            i = self._comment_table.item(row, column).text()
            c = self._run.comments[row]
            c = replace(
                c,
                author=i if column == 1 else c.author,
                text=i if column == 2 else c.text,
            )
            self._change_comment(conn, c)

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
            self._db.add_comment(
                conn,
                self._selected_run,
                self._comment_author.text(),
                self._comment_input.text(),
            )
            self._refresh()
            self._comment_input.setText("")
            self.run_changed.emit()

    def _sample_changed(self, new_sample: Optional[int]) -> None:
        with self._context.db.connect() as conn:
            self._db.change_sample(conn, self._selected_run, new_sample)
        self._refresh()
        self.run_changed.emit()
