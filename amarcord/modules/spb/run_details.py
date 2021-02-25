import logging
from typing import TypeVar

from PyQt5 import QtCore, QtGui, QtWidgets

from amarcord.modules.context import Context
from amarcord.modules.spb.colors import color_manual_run_property
from amarcord.modules.spb.comments import Comments
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
from amarcord.qt.rectangle_widget import RectangleWidget

logger = logging.getLogger(__name__)

T = TypeVar("T")


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
                comment_column_layout = QtWidgets.QVBoxLayout()
                comment_column.setLayout(comment_column_layout)
                self._comments = Comments(self._db)
                self._comments.comments_changed.connect(self._comments_changed)
                comment_column_layout.addWidget(self._comments)

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

    def _comments_changed(self) -> None:
        self.run_changed.emit()

    def _slot_tree_filter_changed(self, new_filter: str) -> None:
        self._details_tree.clear()
        if self._run.karabo is None:
            return
        self._details_tree.insertTopLevelItems(
            0,
            _dict_to_items(
                _filter_dict(_preprocess_dict(self._run.karabo[0]), new_filter),
                parent=None,
            ),
        )
        if self._tree_filter_line.text():
            mf = QtCore.Qt.MatchContains | QtCore.Qt.MatchRecursive
            for i in self._details_tree.findItems(self._tree_filter_line.text(), mf):  # type: ignore
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

            self._comments.set_comments(
                self._run.properties[self._db.tables.property_run_id],
                self._run.properties[self._db.tables.property_comments],
            )

            self._slot_tree_filter_changed(self._tree_filter_line.text())
            self._details_tree.resizeColumnToContents(0)
            self._details_tree.resizeColumnToContents(1)

            self._metadata_table.run_changed(self._run)
            self._metadata_table.model().dataChanged.connect(
                lambda idxfrom, idxto: self.run_changed.emit()
            )
