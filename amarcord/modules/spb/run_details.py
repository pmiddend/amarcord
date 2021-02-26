import logging
from dataclasses import replace
from typing import Any, Optional, cast

from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import QVariant

from amarcord.modules.context import Context
from amarcord.modules.spb.colors import color_manual_run_property
from amarcord.modules.spb.comments import Comments
from amarcord.modules.spb.new_run_dialog import new_run_dialog
from amarcord.modules.spb.proposal_id import ProposalId
from amarcord.modules.spb.queries import Comment, Connection, Run, SPBQueries
from amarcord.modules.spb.run_details_metadata import MetadataTable
from amarcord.modules.spb.run_details_tree import (
    RunDetailsTree,
    _dict_to_items,
    _filter_dict,
    _preprocess_dict,
)
from amarcord.modules.spb.run_id import RunId
from amarcord.modules.spb.run_property import RunProperty
from amarcord.modules.spb.tables import CustomRunPropertyType, Tables
from amarcord.qt.combo_box import ComboBox
from amarcord.qt.debounced_line_edit import DebouncedLineEdit
from amarcord.qt.rectangle_widget import RectangleWidget

logger = logging.getLogger(__name__)


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

                self._run_selector = ComboBox(
                    [(str(r.run_id), QVariant(r.run_id)) for r in self._run_ids]
                )
                self._run_selector.item_selected.connect(self._slot_current_run_changed)
                top_layout.addWidget(QtWidgets.QLabel("Run:"))
                top_layout.addWidget(self._run_selector)
                self._switch_to_latest_button = QtWidgets.QPushButton(
                    self.style().standardIcon(QtWidgets.QStyle.SP_MediaSeekForward),
                    "Switch to latest",
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
                manual_creation = QtWidgets.QPushButton(
                    self.style().standardIcon(QtWidgets.QStyle.SP_FileDialogNewFolder),
                    "New Run",
                )
                manual_creation.clicked.connect(self._slot_new_run)
                top_layout.addWidget(manual_creation)

                comment_column = QtWidgets.QGroupBox("Comments")
                comment_column_layout = QtWidgets.QVBoxLayout()
                comment_column.setLayout(comment_column_layout)
                self._comments = Comments(
                    self._slot_delete_comment,
                    self._slot_change_comment,
                    self._slot_add_comment,
                )
                self._comments.comments_changed.connect(self._comments_changed)
                comment_column_layout.addWidget(self._comments)

                additional_data_column = QtWidgets.QGroupBox("Metadata")

                self._metadata_table = MetadataTable(self._property_change)
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
                custom_column_button = QtWidgets.QPushButton("New custom column")
                custom_column_button.setIcon(
                    self.style().standardIcon(QtWidgets.QStyle.SP_FileDialogNewFolder)
                )
                custom_column_button.clicked.connect(self._slot_new_custom_column)
                additional_data_layout.addWidget(custom_column_button)
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

                self._run: Optional[Run] = None
                self._run_changed(conn, max(r.run_id for r in self._run_ids))

    def _slot_delete_comment(self, comment_id: int) -> None:
        with self._db.connect() as conn:
            self._db.delete_comment(conn, comment_id)
            self._run_changed(conn)

    def _slot_change_comment(self, comment: Comment) -> None:
        with self._db.connect() as conn:
            self._db.change_comment(conn, comment)
            self._run_changed(conn)

    def _slot_add_comment(self, comment: Comment) -> None:
        with self._db.connect() as conn:
            selected_run = self.selected_run_id()
            assert selected_run is not None, "Tried to add a comment, but have no run"
            self._db.add_comment(conn, selected_run, comment.author, comment.text)
            self._run_changed(conn)

    def _property_change(self, property: RunProperty, new_value: Any) -> None:
        assert self._run is not None, "Got a property change but have no run"
        with self._db.connect() as conn:
            selected_run = self.selected_run_id()
            assert (
                selected_run is not None
            ), "Tried to change a run property, but have no run"
            self._db.update_run_property(
                conn,
                selected_run,
                property,
                new_value,
            )
            self._run_changed(conn)
            self.run_changed.emit()

    def _slot_new_custom_column(self) -> None:
        dialog = QtWidgets.QDialog(self)
        dialog_layout = QtWidgets.QVBoxLayout()
        dialog.setLayout(dialog_layout)

        form = QtWidgets.QFormLayout()
        name_input = QtWidgets.QLineEdit()
        name_input.setValidator(
            QtGui.QRegExpValidator(QtCore.QRegExp(r"[a-zA-Z_][a-zA-Z_0-9]*"))
        )
        form.addRow("Name", name_input)

        type_combo = QtWidgets.QComboBox()
        type_combo.addItems([f.name for f in CustomRunPropertyType])
        form.addRow("Type", type_combo)
        dialog_layout.addLayout(form)

        # FIXME: add check for empty name and existing column!
        # also add initial value maybe?

        button_box = QtWidgets.QDialogButtonBox(  # type: ignore
            QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel
        )
        button_box.accepted.connect(dialog.accept)
        button_box.rejected.connect(dialog.reject)

        dialog_layout.addWidget(button_box)

        if dialog.exec() == QtWidgets.QDialog.Rejected:
            return None

        with self._db.connect() as conn:
            self._db.add_custom_run_property(
                conn, name_input.text(), CustomRunPropertyType[type_combo.currentText()]
            )

            assert self._run is not None, "Tried to add a new property, but have no run"
            self._metadata_table.data_changed(
                self._run,
                self._db.run_property_metadata(conn),
                self._db.tables,
                self._sample_ids,
                self._tags,
            )

    def _comments_changed(self) -> None:
        self.run_changed.emit()

    def _slot_tree_filter_changed(self, new_filter: str) -> None:
        assert self._run is not None, "Tried to change karabo filter, but have no run"
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

    def _slot_current_run_changed(self, run_id: int) -> None:
        with self._db.connect() as conn:
            self._run_changed(conn, RunId(run_id))

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
        with self._db.connect() as conn:
            self._run_changed(conn, run_id)

    def _run_selector_changed(self, conn: Connection) -> None:
        new_run_ids = self._db.retrieve_run_ids(conn, self._proposal_id)

        if new_run_ids != self._run_ids:
            self._run_ids = new_run_ids
            self._run_selector.blockSignals(True)
            self._run_selector.clear()
            self._run_selector.addItems([str(r.run_id) for r in self._run_ids])

        if self._run is not None:
            self._run_selector.setCurrentText(
                str(self._run.properties[self._db.tables.property_run_id])
            )
        self._run_selector.blockSignals(False)

    def _run_changed(self, conn: Connection, run_id: Optional[RunId] = None) -> None:
        selected_run_id = self.selected_run_id()
        assert (
            run_id is not None or selected_run_id is not None
        ), "Either give a run ID or have a run present already"
        old_karabo = self._run.karabo if self._run is not None else None
        run_id = cast(RunId, run_id if run_id is not None else selected_run_id)
        self._run = self._db.retrieve_run(conn, run_id)
        self._run = replace(
            self._run,
            karabo=self._db.retrieve_karabo(conn, run_id)
            if old_karabo is None
            else old_karabo,
        )

        self._run_selector_changed(conn)

        self._comments.set_comments(
            self._run.properties[self._db.tables.property_run_id],
            self._run.properties[self._db.tables.property_comments],
        )

        self._slot_tree_filter_changed(self._tree_filter_line.text())
        self._details_tree.resizeColumnToContents(0)
        self._details_tree.resizeColumnToContents(1)

        self._metadata_table.data_changed(
            self._run,
            self._db.run_property_metadata(conn),
            self._db.tables,
            self._sample_ids,
            self._tags,
        )

    def selected_run_id(self) -> Optional[RunId]:
        return (
            RunId(self._run.properties[self._db.tables.property_run_id])
            if self._run is not None
            else None
        )
