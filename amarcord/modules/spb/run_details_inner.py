import logging
from typing import Any
from typing import Dict
from typing import Final
from typing import List
from typing import Optional

from PyQt5 import QtCore
from PyQt5 import QtWidgets
from PyQt5.QtCore import QTimer
from PyQt5.QtCore import QVariant
from PyQt5.QtCore import pyqtSignal
from PyQt5.QtWidgets import QCheckBox
from PyQt5.QtWidgets import QHBoxLayout
from PyQt5.QtWidgets import QPushButton
from PyQt5.QtWidgets import QSizePolicy
from PyQt5.QtWidgets import QSplitter
from PyQt5.QtWidgets import QStyle
from PyQt5.QtWidgets import QWidget

from amarcord.db.attributo_id import AttributoId
from amarcord.db.comment import DBComment
from amarcord.db.db import DBRun
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.karabo import Karabo
from amarcord.db.raw_attributi_map import RawAttributiMap
from amarcord.db.tables import DBTables
from amarcord.modules.spb.attributi_table import AttributiTable
from amarcord.modules.spb.colors import COLOR_MANUAL_ATTRIBUTO
from amarcord.modules.spb.comments import Comments
from amarcord.modules.spb.run_details_tree import RunDetailsTree
from amarcord.modules.spb.run_details_tree import _dict_to_items
from amarcord.modules.spb.run_details_tree import _filter_dict
from amarcord.modules.spb.run_details_tree import _preprocess_dict
from amarcord.qt.combo_box import ComboBox
from amarcord.qt.debounced_line_edit import DebouncedLineEdit
from amarcord.qt.rectangle_widget import RectangleWidget

AUTO_REFRESH_TIMER_MSEC: Final = 5000


logger = logging.getLogger(__name__)


def _refresh_button(style: QStyle) -> QPushButton:
    return QPushButton(
        style.standardIcon(QtWidgets.QStyle.SP_BrowserReload),
        "Refresh",
    )


class RunDetailsInner(QWidget):
    current_run_changed = pyqtSignal(int)
    refresh = pyqtSignal()
    comment_delete = pyqtSignal(int)
    comment_add = pyqtSignal(DBComment)
    comment_changed = pyqtSignal(DBComment)
    attributo_change = pyqtSignal(str, QVariant)
    new_attributo = pyqtSignal()
    manual_new_run = pyqtSignal()

    def __init__(
        self,
        tables: DBTables,
        run_ids: List[int],
        sample_ids: List[int],
        run: DBRun,
        karabo: Optional[Karabo],
        runs_metadata: Dict[AttributoId, DBAttributo],
        parent: Optional[QWidget] = None,
    ) -> None:
        super().__init__(parent)

        self.tables = tables
        self.run = run
        self.karabo = karabo
        self.runs_metadata = runs_metadata
        self.run_ids = run_ids
        self.sample_ids = sample_ids
        self._prior_filter: Optional[str] = None

        top_row = QWidget()
        top_layout = QHBoxLayout(top_row)

        selected_run_id = max(r for r in self.run_ids)
        self._run_selector = ComboBox[int](
            [(str(r), r) for r in self.run_ids],  # type: ignore
            selected=selected_run_id,  # type: ignore
        )
        self._run_selector.item_selected.connect(self.current_run_changed.emit)
        refresh_button = _refresh_button(self.style())
        refresh_button.clicked.connect(self.refresh.emit)
        top_layout.addWidget(refresh_button)
        auto_refresh = QCheckBox("Auto refresh")
        auto_refresh.setChecked(True)
        auto_refresh.clicked.connect(self._slot_toggle_auto_refresh)
        top_layout.addWidget(auto_refresh)
        top_layout.addWidget(QtWidgets.QLabel("Run:"))
        top_layout.addWidget(self._run_selector)
        self._switch_to_latest_button = QtWidgets.QPushButton(
            self.style().standardIcon(QtWidgets.QStyle.SP_MediaSeekForward),
            "Switch to latest",
        )
        self._switch_to_latest_button.clicked.connect(self._slot_switch_to_latest)
        top_layout.addWidget(self._switch_to_latest_button)
        self._auto_switch_to_latest = QtWidgets.QCheckBox("Auto switch to latest")
        self._auto_switch_to_latest.toggled.connect(self._toggle_auto_switch_to_latest)
        top_layout.addWidget(self._auto_switch_to_latest)
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
        manual_creation.clicked.connect(self.manual_new_run.emit)
        top_layout.addWidget(manual_creation)

        comment_column = QtWidgets.QGroupBox("Comments")
        comment_column_layout = QtWidgets.QVBoxLayout(comment_column)
        self._comments = Comments(
            self.comment_delete.emit, self.comment_changed.emit, self.comment_add.emit
        )
        comment_column_layout.addWidget(self._comments)

        additional_data_column = QtWidgets.QGroupBox("Metadata")

        self._attributi_table = AttributiTable(
            self._augmented_attributi(),
            self.runs_metadata,
            self.sample_ids,
            self.attributo_change.emit,
        )
        additional_data_layout = QtWidgets.QVBoxLayout()
        additional_data_layout.addWidget(self._attributi_table)
        table_legend_layout = QtWidgets.QHBoxLayout()
        table_legend_layout.addStretch()
        table_legend_layout.addWidget(RectangleWidget(COLOR_MANUAL_ATTRIBUTO))
        table_legend_layout.addWidget(QtWidgets.QLabel("<i>manually edited</i>"))
        table_legend_layout.addStretch()
        attributo_button = QtWidgets.QPushButton(
            self.style().standardIcon(QtWidgets.QStyle.SP_FileDialogNewFolder),
            "New attributo",
        )
        attributo_button.clicked.connect(self.new_attributo.emit)
        additional_data_layout.addLayout(table_legend_layout)
        additional_data_layout.addWidget(attributo_button)
        additional_data_column.setLayout(additional_data_layout)

        self._root_layout = QtWidgets.QVBoxLayout()
        self.setLayout(self._root_layout)

        self._root_layout.addWidget(top_row)

        root_splitter = QSplitter()
        root_splitter.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self._root_layout.addWidget(root_splitter)

        details_box = QtWidgets.QGroupBox("Run details")
        details_column_layout = QtWidgets.QVBoxLayout(details_box)
        self._details_tree = RunDetailsTree()
        details_column_layout.addWidget(self._details_tree)

        right_column = QtWidgets.QWidget()
        right_column_layout = QtWidgets.QVBoxLayout(right_column)
        right_column_layout.addWidget(comment_column)
        right_column_layout.addWidget(details_box)

        tree_search_row = QtWidgets.QHBoxLayout()
        tree_search_row.addWidget(QtWidgets.QLabel("Filter:"))
        self._tree_filter_line = DebouncedLineEdit()
        self._tree_filter_line.setClearButtonEnabled(True)
        self._tree_filter_line.textChanged.connect(self._slot_tree_filter_changed)
        tree_search_row.addWidget(self._tree_filter_line)
        details_column_layout.addLayout(tree_search_row)

        root_splitter.addWidget(additional_data_column)
        root_splitter.addWidget(right_column)

        self.run_changed(self.run, self.karabo, run_ids, sample_ids, runs_metadata)

        self._update_timer = QTimer(self)
        self._update_timer.timeout.connect(self._timed_refresh)

    def _augmented_attributi(self) -> RawAttributiMap:
        copied = self.run.attributi.copy()
        copied.set_single_manual(
            self.tables.attributo_run_sample_id, self.run.sample_id
        )
        return copied

    def hideEvent(self, _e: Any) -> None:
        self._update_timer.stop()

    def showEvent(self, _e: Any) -> None:
        self.refresh.emit()
        self._update_timer.start(AUTO_REFRESH_TIMER_MSEC)

    def _toggle_auto_switch_to_latest(self, new_state: bool) -> None:
        max_run_id = max(self.run_ids)
        if new_state and self.run.id != max_run_id:
            self.current_run_changed.emit(max_run_id)

    def _slot_switch_to_latest(self) -> None:
        self.current_run_changed.emit(max(r for r in self.run_ids))

    def _timed_refresh(self) -> None:
        self.refresh.emit()

    def _slot_toggle_auto_refresh(self) -> None:
        if self._update_timer.isActive():
            self._update_timer.stop()
        else:
            self._update_timer.start(AUTO_REFRESH_TIMER_MSEC)

    def runs_metadata_changed(
        self, new_runs_metadata: Dict[AttributoId, DBAttributo]
    ) -> None:
        self.runs_metadata = new_runs_metadata
        self._attributi_table.data_changed(
            self._augmented_attributi(),
            self.runs_metadata,
            self.sample_ids,
        )

    def run_changed(
        self,
        new_run: DBRun,
        new_karabo: Optional[Karabo],
        new_run_ids: List[int],
        new_sample_ids: List[int],
        new_metadata: Dict[AttributoId, DBAttributo],
    ) -> None:
        old_run_modified = self.run.modified
        run_was_modified = old_run_modified != new_run.modified
        self.run = new_run
        metadata_was_modified = self.runs_metadata != new_metadata
        self.karabo = new_karabo
        self.runs_metadata = new_metadata

        old_run_ids = self.run_ids
        self.run_ids = new_run_ids
        sample_ids_was_modified = new_sample_ids != self.sample_ids
        self.sample_ids = new_sample_ids

        if old_run_ids != new_run_ids:
            self._run_selector.reset_items([(str(s), s) for s in new_run_ids])

            max_run_id = max(self.run_ids)
            if self.run.id != max_run_id:
                self.current_run_changed.emit(max_run_id)

        self._run_selector.set_current_value(new_run.id)

        self._comments.set_comments(self.run.id, self.run.comments)

        self._prior_filter = None
        self._slot_tree_filter_changed(self._tree_filter_line.text())
        self._details_tree.resizeColumnToContents(0)
        self._details_tree.resizeColumnToContents(1)

        if run_was_modified or metadata_was_modified or sample_ids_was_modified:
            self._attributi_table.data_changed(
                self._augmented_attributi(),
                self.runs_metadata,
                self.sample_ids,
            )

        self._switch_to_latest_button.setEnabled(self.run.id != max(self.run_ids))

    def _slot_tree_filter_changed(self, new_filter: str) -> None:
        if new_filter == self._prior_filter:
            return
        self._prior_filter = new_filter
        self._details_tree.clear()
        if self.karabo is None:
            return
        self._details_tree.insertTopLevelItems(
            0,
            _dict_to_items(
                _filter_dict(_preprocess_dict(self.karabo[0]), new_filter),
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
        self._details_tree.resizeColumnToContents(1)
