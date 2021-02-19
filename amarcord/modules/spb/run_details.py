from dataclasses import replace
import datetime
import getpass
import logging
from typing import Optional
from typing import Dict
from typing import List
from typing import Any
import humanize
import numpy as np
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


def _recurse_to_items(new_item: QtWidgets.QTreeWidgetItem, k: str, v: Any) -> None:
    if isinstance(v, dict):
        new_item.setText(1, "Dictionary")
        _dict_to_items(v, new_item)
    elif isinstance(v, list):
        new_item.setText(1, "List")
        _list_to_items(v, new_item)
    elif isinstance(v, (bool, str, int, float)):
        if k == "timestamp":
            new_item.setText(
                1, str(datetime.datetime.fromtimestamp(v / 1000 / 1000 / 1000))
            )
        else:
            new_item.setText(1, str(v))
    elif isinstance(v, np.ndarray):
        new_item.setText(1, "Array " + str(v.shape))
    else:
        new_item.setText(1, str(type(v)))


def _preprocess_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    def _maybe_recurse(v: Any) -> Any:
        if isinstance(v, dict):
            return _preprocess_dict(v)
        return v

    result: Dict[str, Any] = {}
    for k, v in d.items():
        if "." not in k:
            if isinstance(v, dict):
                result[k] = _preprocess_dict(v)
            else:
                result[k] = v
        else:
            parts = k.split(".")
            base = result
            for part in parts[:-1]:
                if part not in base:
                    base[part] = {}
                base = base[part]
            base[parts[-1]] = v
    return result


def _list_to_items(
    d: List[Any], parent: Optional[QtWidgets.QTreeWidgetItem]
) -> List[QtWidgets.QTreeWidgetItem]:
    items: List[QtWidgets.QTreeWidgetItem] = []
    for idx, v in enumerate(d):
        new_item = QtWidgets.QTreeWidgetItem(parent)
        new_item.setText(0, str(idx))
        _recurse_to_items(new_item, idx, v)
        items.append(new_item)
    return items


def _dict_to_items(
    d: Dict[str, Any], parent: Optional[QtWidgets.QTreeWidgetItem]
) -> List[QtWidgets.QTreeWidgetItem]:
    items: List[QtWidgets.QTreeWidgetItem] = []
    for k, v in d.items():
        new_item = QtWidgets.QTreeWidgetItem(parent)
        new_item.setText(0, k)
        _recurse_to_items(new_item, k, v)
        items.append(new_item)
    return items


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
                self._run_selector.currentTextChanged.connect(
                    self._slot_current_run_changed
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
                manual_creation = QtWidgets.QPushButton("New Run")
                manual_creation.setIcon(
                    self.style().standardIcon(QtWidgets.QStyle.SP_FileDialogNewFolder)
                )
                manual_creation.clicked.connect(self._slot_new_run)
                top_layout.addWidget(manual_creation)

                comment_column = QtWidgets.QGroupBox("Comments")
                comment_layout = QtWidgets.QVBoxLayout()
                comment_column.setLayout(comment_layout)
                self._comment_table = _CommentTable()
                self._comment_table.setSelectionBehavior(
                    # pylint: disable=no-member
                    QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows  # type: ignore
                )
                self._comment_table.delete_current_row.connect(
                    self._slot_delete_comment
                )

                comment_layout.addWidget(self._comment_table)

                comment_form_layout = QtWidgets.QFormLayout()
                self._comment_author = QtWidgets.QLineEdit()
                self._comment_author.setText(getpass.getuser())
                self._comment_author.textChanged.connect(self._slot_author_changed)
                comment_form_layout.addRow(
                    QtWidgets.QLabel("Author"), self._comment_author
                )
                self._comment_input = QtWidgets.QLineEdit()
                self._comment_input.setClearButtonEnabled(True)
                self._comment_input.textChanged.connect(self._slot_comment_text_changed)
                self._comment_input.returnPressed.connect(self._slot_add_comment)
                comment_form_layout.addRow(
                    QtWidgets.QLabel("Text"), self._comment_input
                )
                self._add_comment_button = QtWidgets.QPushButton("Add Comment")
                self._add_comment_button.setIcon(
                    self.style().standardIcon(QtWidgets.QStyle.SP_DialogOkButton)
                )
                self._add_comment_button.setEnabled(False)
                self._add_comment_button.clicked.connect(self._slot_add_comment)
                comment_form_layout.addWidget(self._add_comment_button)
                comment_layout.addLayout(comment_form_layout)

                additional_data_column = QtWidgets.QGroupBox("Metadata")

                additional_data_layout = QtWidgets.QFormLayout()
                additional_data_column.setLayout(additional_data_layout)
                self._sample_chooser = QtWidgets.QComboBox()
                self._sample_chooser.addItems(
                    [str(s) for s in self._sample_ids] + ["None"]
                )
                self._sample_chooser.currentTextChanged.connect(
                    lambda new_sample_str: self._sample_changed(
                        int(new_sample_str) if new_sample_str != "None" else None
                    )
                )
                additional_data_layout.addRow(
                    QtWidgets.QLabel("Sample"), self._sample_chooser
                )
                self._tags_widget = Tags()
                self._tags_widget.completion(self._tags)
                self._tags_widget.tagsEdited.connect(self._slot_tags_changed)
                additional_data_layout.addRow(
                    QtWidgets.QLabel("Tags"), self._tags_widget
                )

                root_layout = QtWidgets.QVBoxLayout()
                self.setLayout(root_layout)

                root_layout.addWidget(top_row)

                root_columns = QtWidgets.QHBoxLayout()
                root_layout.addLayout(root_columns)

                editable_column = QtWidgets.QWidget()
                editable_column_layout = QtWidgets.QVBoxLayout()
                editable_column.setLayout(editable_column_layout)
                editable_column_layout.addWidget(additional_data_column)
                editable_column_layout.addWidget(comment_column)
                editable_column_layout.addStretch()

                details_column = QtWidgets.QGroupBox("Run details")
                details_column_layout = QtWidgets.QVBoxLayout()
                details_column.setLayout(details_column_layout)
                self._details_tree = QtWidgets.QTreeWidget()
                self._details_tree.setColumnCount(2)
                self._details_tree.setHeaderLabels(["Key", "Type or value"])
                details_column_layout.addWidget(self._details_tree)

                root_columns.addWidget(editable_column)
                root_columns.addWidget(details_column)
                root_columns.setStretch(0, 1)
                root_columns.setStretch(1, 3)

                self._selected_run = RunId(-1)
                self._run_changed(max(r.run_id for r in self._run_ids))

    def _slot_current_run_changed(self, new_run_id: str) -> None:
        self._run_changed(RunId(int(new_run_id)))

    def _slot_new_run(self) -> None:
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

    def _slot_delete_comment(self) -> None:
        with self._context.db.connect() as conn:
            self._db.delete_comment(
                conn,
                self._run.comments[self._comment_table.currentRow()].id,
            )
            self._refresh()
            self.run_changed.emit()

    def _slot_tags_changed(self) -> None:
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
            self._details_tree.clear()
            self._details_tree.insertTopLevelItems(
                0, _dict_to_items(_preprocess_dict(self._run.karabo[0]), None)
            )
            self._details_tree.resizeColumnToContents(0)
            self._details_tree.resizeColumnToContents(1)

    def _comment_cell_changed(self, row: int, column: int) -> None:
        with self._context.db.connect() as conn:
            i = self._comment_table.item(row, column).text()
            c = self._run.comments[row]
            c = replace(
                c,
                author=i if column == 1 else c.author,
                text=i if column == 2 else c.text,
            )
            self._db.change_comment(conn, c)

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
