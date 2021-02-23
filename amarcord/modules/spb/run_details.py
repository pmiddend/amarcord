import datetime
import getpass
import logging
from dataclasses import replace
from typing import Optional, TypeVar

import humanize
from PyQt5 import QtCore, QtGui, QtWidgets

from amarcord.modules.context import Context
from amarcord.modules.spb.colors import color_manual_run_property
from amarcord.modules.spb.run_property import (
    RunProperty,
)
from amarcord.modules.spb.new_run_dialog import new_run_dialog
from amarcord.modules.spb.proposal_id import ProposalId
from amarcord.modules.spb.queries import SPBQueries
from amarcord.modules.spb.run_details_metadata import MetadataTable
from amarcord.modules.spb.run_details_tree import (
    RunDetailsTree,
    _dict_to_items,
    _filter_dict,
    _preprocess_dict,
)
from amarcord.modules.spb.run_id import RunId
from amarcord.modules.spb.tables import Tables
from amarcord.qt.debounced_line_edit import DebouncedLineEdit
from amarcord.qt.tags import Tags

logger = logging.getLogger(__name__)

T = TypeVar("T")


class RectangleWidget(QtWidgets.QWidget):
    def __init__(
        self, color: QtGui.QColor, parent: Optional[QtWidgets.QWidget] = None
    ) -> None:
        super().__init__(parent)

        self._color = color
        font = QtWidgets.QApplication.font()
        metrics = QtGui.QFontMetrics(font)
        r = metrics.boundingRect("W").size()
        self._size = QtCore.QSize(r.height(), r.height())

    def paintEvent(self, e: QtGui.QPaintEvent) -> None:
        p = QtGui.QPainter(self)

        rect = self.rect()
        height = rect.height()
        p.fillRect(
            QtCore.QRect(rect.topLeft(), QtCore.QSize(height, height)), self._color
        )

    def sizeHint(self) -> QtCore.QSize:
        return self._size


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
                # noinspection PyUnresolvedReferences
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

                self._metadata_table = MetadataTable(self._db)

                additional_data_layout = QtWidgets.QVBoxLayout()
                additional_data_layout.addWidget(self._metadata_table)
                table_legend_layout = QtWidgets.QHBoxLayout()
                table_legend_layout.addStretch()
                table_legend_layout.addWidget(
                    RectangleWidget(color_manual_run_property)
                )
                table_legend_layout.addWidget(
                    QtWidgets.QLabel("<i>manually edited</i>")
                )
                table_legend_layout.addStretch()
                additional_data_layout.addLayout(table_legend_layout)
                additional_data_column.setLayout(additional_data_layout)

                self._tags_widget = Tags()
                self._tags_widget.completion(self._tags)
                self._tags_widget.tagsEdited.connect(self._slot_tags_changed)

                root_layout = QtWidgets.QVBoxLayout()
                self.setLayout(root_layout)

                root_layout.addWidget(top_row)

                root_columns = QtWidgets.QHBoxLayout()
                root_layout.addLayout(root_columns)

                editable_column = QtWidgets.QWidget()
                editable_column_layout = QtWidgets.QVBoxLayout()
                editable_column.setLayout(editable_column_layout)
                editable_column_layout.addWidget(comment_column)
                editable_column_layout.addWidget(additional_data_column)

                details_column = QtWidgets.QGroupBox("Run details")
                details_column_layout = QtWidgets.QVBoxLayout()
                details_column.setLayout(details_column_layout)
                self._details_tree = RunDetailsTree()
                details_column_layout.addWidget(self._details_tree)

                tree_search_row = QtWidgets.QHBoxLayout()
                tree_search_row.addWidget(QtWidgets.QLabel("Filter:"))
                self._tree_filter_line = DebouncedLineEdit()
                self._tree_filter_line.setClearButtonEnabled(True)
                self._tree_filter_line.textChanged.connect(
                    self._slot_tree_filter_changed
                )
                tree_search_row.addWidget(self._tree_filter_line)
                details_column_layout.addLayout(tree_search_row)

                root_columns.addWidget(editable_column)
                root_columns.addWidget(details_column)
                root_columns.setStretch(0, 2)
                root_columns.setStretch(1, 3)

                self._selected_run = RunId(-1)
                self._run_changed(max(r.run_id for r in self._run_ids))

    def _slot_tree_filter_changed(self, new_filter: str) -> None:
        self._details_tree.clear()
        self._details_tree.insertTopLevelItems(
            0,
            _dict_to_items(
                _filter_dict(_preprocess_dict(self._run.karabo[0]), new_filter),
                parent=None,
            ),
        )
        if self._tree_filter_line.text():
            # noinspection PyUnresolvedReferences
            for i in self._details_tree.findItems(
                self._tree_filter_line.text(),
                QtCore.Qt.MatchFlag.MatchContains | QtCore.Qt.MatchFlag.MatchRecursive,
            ):
                i.setExpanded(True)
                p = i.parent()
                while p is not None:
                    p.setExpanded(True)
                    p = p.parent()

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
                self._run.properties[RunProperty.COMMENTS][
                    self._comment_table.currentRow()
                ].id,
            )
            self._refresh()
            self.run_changed.emit()

    def _slot_tags_changed(self) -> None:
        if self._tags_widget.tagsStr() == self._run.properties[RunProperty.TAGS]:
            return
        with self._context.db.connect() as conn:
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
            # self._sample_chooser.setCurrentText(
            #     str(self._run.properties[RunProperty.SAMPLE])
            #     if self._run.properties[RunProperty.SAMPLE] is not None
            #     else "None"
            # )
            # self._tags_widget.tags(self._run.properties[RunProperty.TAGS])
            self._comment_table.setColumnCount(3)
            self._comment_table.setRowCount(
                len(self._run.properties[RunProperty.COMMENTS])
            )
            self._comment_table.setHorizontalHeaderLabels(["Created", "Author", "Text"])
            self._comment_table.horizontalHeader().setStretchLastSection(True)
            self._comment_table.verticalHeader().hide()

            now = datetime.datetime.utcnow()
            for row, c in enumerate(self._run.properties[RunProperty.COMMENTS]):
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
            self._slot_tree_filter_changed(self._tree_filter_line.text())
            self._details_tree.resizeColumnToContents(0)
            self._details_tree.resizeColumnToContents(1)

            self._metadata_table.run_changed(self._run)
            self._metadata_table.model().dataChanged.connect(
                lambda idxfrom, idxto: self.run_changed.emit()
            )

    def _comment_cell_changed(self, row: int, column: int) -> None:
        with self._context.db.connect() as conn:
            i = self._comment_table.item(row, column).text()
            c = self._run.properties[RunProperty.COMMENTS][row]
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
