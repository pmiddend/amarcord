import logging
from typing import Any
from typing import Dict
from typing import Final
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple

from PyQt5 import QtWidgets
from PyQt5.QtCore import QTimer
from PyQt5.QtCore import QVariant
from PyQt5.QtCore import pyqtSignal
from PyQt5.QtGui import QValidator
from PyQt5.QtWidgets import QCheckBox
from PyQt5.QtWidgets import QHBoxLayout
from PyQt5.QtWidgets import QLineEdit
from PyQt5.QtWidgets import QPushButton
from PyQt5.QtWidgets import QSizePolicy
from PyQt5.QtWidgets import QSplitter
from PyQt5.QtWidgets import QStyle
from PyQt5.QtWidgets import QWidget

from amarcord.db.attributo_id import AttributoId
from amarcord.db.comment import DBComment
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.karabo import Karabo
from amarcord.db.mini_sample import DBMiniSample
from amarcord.db.raw_attributi_map import RawAttributiMap
from amarcord.db.table_classes import DBRun
from amarcord.db.tables import DBTables
from amarcord.modules.spb.attributi_table import AttributiTable
from amarcord.modules.spb.colors import COLOR_MANUAL_ATTRIBUTO
from amarcord.modules.spb.comments import Comments
from amarcord.qt.rectangle_widget import RectangleWidget
from amarcord.util import str_to_int

AUTO_REFRESH_TIMER_MSEC: Final = 5000


logger = logging.getLogger(__name__)


def _refresh_button(style: QStyle) -> QPushButton:
    return QPushButton(
        style.standardIcon(QtWidgets.QStyle.SP_BrowserReload),
        "Refresh",
    )


class _RunIdValidator(QValidator):
    def __init__(self, run_ids: Iterable[int]) -> None:
        super().__init__()
        self._run_ids = set(run_ids)

    def reset_run_ids(self, run_ids: Iterable[int]) -> None:
        self._run_ids = set(run_ids)

    def validate(self, input_: str, pos: int) -> Tuple["QValidator.State", str, int]:
        if input_ == "":
            return QValidator.Intermediate, input_, pos
        id_to_int = str_to_int(input_)
        if id_to_int is None:
            return QValidator.Invalid, input_, pos
        if id_to_int in self._run_ids:
            return QValidator.Acceptable, input_, pos
        return QValidator.Intermediate, input_, pos


class RunDetailsInner(QWidget):
    current_run_changed = pyqtSignal(int)
    refresh = pyqtSignal()
    comment_delete = pyqtSignal(int)
    comment_add = pyqtSignal(DBComment)
    comment_changed = pyqtSignal(DBComment)
    attributo_change = pyqtSignal(str, QVariant)
    attributo_manual_remove = pyqtSignal(str)
    new_attributo = pyqtSignal()
    manual_new_run = pyqtSignal()

    def __init__(
        self,
        tables: DBTables,
        run_ids: List[int],
        samples: List[DBMiniSample],
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
        self.sample_ids = samples

        top_row = QWidget()
        top_layout = QHBoxLayout(top_row)
        top_layout.setContentsMargins(0, 0, 0, 0)

        selected_run_id = max(r for r in self.run_ids)
        self._run_selector = QLineEdit(str(selected_run_id))
        self._run_id_validator = _RunIdValidator(self.run_ids)
        self._run_selector.setValidator(self._run_id_validator)
        self._run_selector.editingFinished.connect(
            lambda: self.current_run_changed.emit(int(self._run_selector.text()))
        )
        refresh_button = _refresh_button(self.style())
        refresh_button.clicked.connect(self.refresh.emit)
        top_layout.addWidget(refresh_button)
        self._auto_refresh = QCheckBox("Auto refresh")
        self._auto_refresh.setChecked(True)
        self._auto_refresh.clicked.connect(self._slot_toggle_auto_refresh)
        top_layout.addWidget(self._auto_refresh)
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
            self.attributo_manual_remove.emit,
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

        right_column = QtWidgets.QWidget()
        right_column_layout = QtWidgets.QVBoxLayout(right_column)
        right_column_layout.addWidget(comment_column)

        root_splitter.addWidget(additional_data_column)
        root_splitter.addWidget(right_column)

        self.run_changed(self.run, self.karabo, run_ids, samples, runs_metadata)

        self._update_timer = QTimer(self)
        self._update_timer.timeout.connect(self._timed_refresh)

    def _augmented_attributi(self) -> RawAttributiMap:
        copied = self.run.attributi.copy()
        if self.run.sample_id is not None:
            copied.set_single_manual(
                self.tables.attributo_run_sample_id, self.run.sample_id
            )
        return copied

    def hideEvent(self, _e: Any) -> None:
        self._update_timer.stop()

    def showEvent(self, _e: Any) -> None:
        if self._auto_refresh.isChecked():
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
        new_samples: List[DBMiniSample],
        new_metadata: Dict[AttributoId, DBAttributo],
    ) -> None:
        old_run_modified = self.run.modified
        run_was_modified = (
            old_run_modified != new_run.modified or new_run.id != self.run.id
        )
        run_id_was_modified = self.run.id != new_run.id
        self.run = new_run
        metadata_was_modified = self.runs_metadata != new_metadata
        self.karabo = new_karabo
        self.runs_metadata = new_metadata

        old_run_ids = self.run_ids
        self.run_ids = new_run_ids
        sample_ids_was_modified = new_samples != self.sample_ids
        self.sample_ids = new_samples

        if old_run_ids != new_run_ids:
            self._run_id_validator.reset_run_ids(new_run_ids)

            if self._auto_switch_to_latest.isChecked():
                max_run_id = max(self.run_ids)
                if self.run.id != max_run_id:
                    self.current_run_changed.emit(max_run_id)

        if run_id_was_modified:
            self._run_selector.setText(str(new_run.id))

        self._comments.set_comments(self.run.id, self.run.comments)

        if run_was_modified or metadata_was_modified or sample_ids_was_modified:
            self._attributi_table.data_changed(
                self._augmented_attributi(),
                self.runs_metadata,
                self.sample_ids,
            )

        is_latest = self.run.id == max(self.run_ids)
        self._switch_to_latest_button.setEnabled(not is_latest)

        # If we've explicitly selected (or changed to otherwise) a run which is not the latest
        # then auto-disable the "switch to latest" logic
        if not is_latest and self._auto_switch_to_latest.isChecked():
            self._auto_switch_to_latest.setChecked(False)
