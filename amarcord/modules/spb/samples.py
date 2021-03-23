import datetime
import getpass
import logging
from dataclasses import replace
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Final
from typing import List
from typing import Optional
from typing import Union
from typing import cast

from PyQt5.QtCore import QModelIndex
from PyQt5.QtCore import QTimer
from PyQt5.QtCore import QUrl
from PyQt5.QtCore import Qt
from PyQt5.QtCore import pyqtSignal
from PyQt5.QtGui import QContextMenuEvent
from PyQt5.QtGui import QDesktopServices
from PyQt5.QtWidgets import QAbstractItemView
from PyQt5.QtWidgets import QCheckBox
from PyQt5.QtWidgets import QFileDialog
from PyQt5.QtWidgets import QFormLayout
from PyQt5.QtWidgets import QHBoxLayout
from PyQt5.QtWidgets import QLabel
from PyQt5.QtWidgets import QMenu
from PyQt5.QtWidgets import QMessageBox
from PyQt5.QtWidgets import QPushButton
from PyQt5.QtWidgets import QSplitter
from PyQt5.QtWidgets import QStyle
from PyQt5.QtWidgets import QTableWidget
from PyQt5.QtWidgets import QTableWidgetItem
from PyQt5.QtWidgets import QVBoxLayout
from PyQt5.QtWidgets import QWidget

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributi import (
    pretty_print_attributo,
)
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.attributo_id import AttributoId
from amarcord.db.constants import MANUAL_SOURCE_NAME
from amarcord.db.db import Connection
from amarcord.db.db import DB
from amarcord.db.db import DBSample
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.raw_attributi_map import RawAttributiMap
from amarcord.db.tables import DBTables
from amarcord.modules.context import Context
from amarcord.modules.json import JSONDict
from amarcord.modules.spb.attributi_table import AttributiTable
from amarcord.qt.combo_box import ComboBox
from amarcord.qt.image_viewer import display_image_viewer
from amarcord.qt.pubchem import validate_pubchem_compound
from amarcord.qt.validated_line_edit import ValidatedLineEdit
from amarcord.qt.validators import Partial
from amarcord.qt.validators import parse_existing_filename
from amarcord.qt.validators import parse_list
from amarcord.util import str_to_int

DATE_TIME_FORMAT = "%Y-%m-%d %H:%M"

NEW_SAMPLE_HEADLINE = "New sample"
AUTO_REFRESH_TIMER_MSEC: Final = 5000

logger = logging.getLogger(__name__)


def _empty_sample(available_attributi: Dict[AttributoId, DBAttributo]) -> DBSample:
    attributi_map: JSONDict = {}
    if AttributoId("created") in available_attributi:
        new_source = cast(JSONDict, attributi_map.get(MANUAL_SOURCE_NAME, {}))
        new_source["created"] = datetime.datetime.utcnow().isoformat()
        attributi_map[MANUAL_SOURCE_NAME] = new_source
    if AttributoId("creator") in available_attributi:
        new_source = cast(JSONDict, attributi_map.get(MANUAL_SOURCE_NAME, {}))
        new_source["creator"] = getpass.getuser()
        attributi_map[MANUAL_SOURCE_NAME] = new_source
    return DBSample(
        id=None,
        target_id=None,
        compounds=None,
        micrograph=None,
        protocol=None,
        attributi=RawAttributiMap(attributi_map),
    )


def _validate_pubchem(s: str) -> Union[int, Partial, None]:
    si = str_to_int(s)
    if si is not None and validate_pubchem_compound(si):
        return si
    return None


class _SampleTable(QTableWidget):
    delete_current_row = pyqtSignal()

    def contextMenuEvent(self, event: QContextMenuEvent) -> None:
        menu = QMenu(self)
        deleteAction = menu.addAction(
            self.style().standardIcon(QStyle.SP_DialogCancelButton),
            "Delete",
        )
        action = menu.exec_(self.mapToGlobal(event.pos()))
        if action == deleteAction:
            self.delete_current_row.emit()


class Samples(QWidget):
    new_attributo = pyqtSignal()

    def __init__(
        self,
        context: Context,
        tables: DBTables,
        parent: Optional[QWidget] = None,
    ) -> None:
        super().__init__(parent)

        self._db = DB(context.db, tables)

        # if "filesystem" not in context.config:
        #     raise Exception('Cannot find "filesystem" in configuration')
        # if "base_path" not in context.config["filesystem"]:
        #     raise Exception('Cannot find "filesystem.base_path" in configuration')
        # if not isinstance(context.config["filesystem"]["base_path"], str):
        #     raise Exception('"filesystem.base_path" in configuration is not a string')
        # self._proposal_file_path = Path(context.config["filesystem"]["base_path"])
        root_layout = QHBoxLayout()
        root_layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(root_layout)

        root_widget = QSplitter(self)
        root_layout.addWidget(root_widget)
        left_column = QWidget()
        left_column_layout = QVBoxLayout(left_column)
        self._sample_table = _SampleTable()
        self._sample_table.delete_current_row.connect(self._slot_delete_sample)
        self._sample_table.doubleClicked.connect(self._slot_row_selected)
        refresh_line_layout = QHBoxLayout()
        refresh_button = QPushButton(
            self.style().standardIcon(QStyle.SP_BrowserReload),
            "Refresh",
        )
        refresh_line_layout.addWidget(refresh_button)
        refresh_button.clicked.connect(self._slot_refresh_with_conn)
        self._auto_refresh = QCheckBox("Auto refresh")
        self._auto_refresh.setChecked(True)
        self._auto_refresh.toggled.connect(self._slot_toggle_auto_refresh)
        refresh_line_layout.addWidget(self._auto_refresh)
        refresh_line_layout.addStretch()
        left_column_layout.addLayout(refresh_line_layout)
        left_column_layout.addWidget(
            QLabel(
                "<b>Double-click</b> row to edit. <b>Right-click</b> column header to show options."
            )
        )
        left_column_layout.addWidget(self._sample_table)
        root_widget.addWidget(left_column)

        right_widget = QWidget()
        right_root_layout = QVBoxLayout()
        right_widget.setLayout(right_root_layout)
        self._right_headline = QLabel(NEW_SAMPLE_HEADLINE)
        self._right_headline.setStyleSheet("font-size: 25pt;")
        self._right_headline.setAlignment(Qt.AlignHCenter)
        right_root_layout.addWidget(self._right_headline)
        right_form_layout = QFormLayout()
        right_root_layout.addLayout(right_form_layout)
        self._log_widget = QLabel()
        self._log_widget.setStyleSheet("QLabel { font: italic; color: green; }")
        self._log_widget.setAlignment(Qt.AlignHCenter)
        right_root_layout.addWidget(self._log_widget)
        right_widget.setLayout(right_root_layout)
        root_widget.addWidget(right_widget)

        self._target_id_edit = ComboBox[Optional[int]](
            items=[("None", None)], selected=None
        )
        right_form_layout.addRow(
            "Target",
            self._target_id_edit,
        )
        self._target_id_edit.item_selected.connect(self._target_id_change)

        self._compounds_edit = ValidatedLineEdit(
            None,
            lambda str_list: ", ".join(str(s) for s in str_list),  # type: ignore
            lambda str_list_str: parse_list(str_list_str, None, _validate_pubchem),  # type: ignore
            "list of pubchem CIDs, separated by commas",
        )
        self._compounds_edit.value_change.connect(self._compounds_change)
        right_form_layout.addRow("Compounds", self._compounds_edit)

        micrograph_layout = QHBoxLayout()
        micrograph_layout.setContentsMargins(0, 0, 0, 0)
        self._micrograph_edit = ValidatedLineEdit(
            None,
            lambda p: p,  # type: ignore
            parse_existing_filename,  # type: ignore
            "Absolute path to file",
        )
        self._micrograph_edit.value_change.connect(self._micrograph_change)
        micrograph_layout.addWidget(self._micrograph_edit)
        choose_micrograph_button = QPushButton(
            self.style().standardIcon(QStyle.SP_DialogOpenButton), "Browse..."
        )
        self._display_micrograph_button = QPushButton(
            self.style().standardIcon(QStyle.SP_FileDialogContentsView), "Show"
        )
        self._display_micrograph_button.setEnabled(False)
        self._display_micrograph_button.clicked.connect(self._display_micrograph)
        choose_micrograph_button.clicked.connect(self._choose_micrograph)
        micrograph_layout.addWidget(self._display_micrograph_button)
        micrograph_layout.addWidget(choose_micrograph_button)
        right_form_layout.addRow("Micrograph", micrograph_layout)

        protocol_layout = QHBoxLayout()
        protocol_layout.setContentsMargins(0, 0, 0, 0)
        self._protocol_edit = ValidatedLineEdit(
            None,
            lambda p: p,  # type: ignore
            parse_existing_filename,  # type: ignore
            "Absolute path to file",
        )
        self._protocol_edit.value_change.connect(self._protocol_change)
        protocol_layout.addWidget(self._protocol_edit)
        choose_protocol_button = QPushButton(
            self.style().standardIcon(QStyle.SP_DialogOpenButton), "Browse..."
        )
        self._open_protocol_button = QPushButton(
            self.style().standardIcon(QStyle.SP_FileDialogContentsView), "Open"
        )
        self._open_protocol_button.setEnabled(False)
        self._open_protocol_button.clicked.connect(self._open_protocol)
        choose_protocol_button.clicked.connect(self._choose_protocol)
        protocol_layout.addWidget(self._open_protocol_button)
        protocol_layout.addWidget(choose_protocol_button)
        right_form_layout.addRow("Protocol", protocol_layout)

        with self._db.connect() as conn:
            metadata_wrapper = QWidget()
            metadata_wrapper_layout = QVBoxLayout(metadata_wrapper)
            available_attributi = self._db.retrieve_table_attributi(
                conn, AssociatedTable.SAMPLE
            )
            self._current_sample = _empty_sample(available_attributi)
            self._attributi_table = AttributiTable(
                self._current_sample.attributi,
                available_attributi,
                [],
                self._attributo_change,
            )
            metadata_wrapper_layout.addWidget(self._attributi_table)
            attributo_button = QPushButton(
                self.style().standardIcon(QStyle.SP_FileDialogNewFolder),
                "New attributo",
            )
            attributo_button.clicked.connect(self.new_attributo.emit)
            metadata_wrapper_layout.addWidget(attributo_button)
            right_form_layout.addRow(metadata_wrapper)
            self._samples: List[DBSample] = []

        self._submit_widget = QWidget()
        self._submit_layout = QHBoxLayout()
        self._submit_layout.setContentsMargins(0, 0, 0, 0)
        self._submit_widget.setLayout(self._submit_layout)
        self._add_button = self._create_add_button()
        self._add_button.setEnabled(True)
        self._submit_layout.addWidget(self._add_button)

        self._samples_with_runs: Dict[int, List[int]] = {}

        self._attributo_manual_changes: Dict[AttributoId, Any] = {}
        right_form_layout.addWidget(self._submit_widget)

        with self._db.connect() as conn:
            self._reload_and_fill_samples(conn)

        self._update_timer = QTimer(self)
        self._update_timer.timeout.connect(self._slot_refresh_with_conn)

    def _slot_toggle_auto_refresh(self, _new_state: bool) -> None:
        if self._update_timer.isActive():
            self._update_timer.stop()
        else:
            self._update_timer.start(AUTO_REFRESH_TIMER_MSEC)

    def _attributo_change(self, attributo: AttributoId, value: Any) -> None:
        # We could immediately change the attribute, but we have this "Save changes" button
        # with self._db.connect() as conn:
        logger.info("Setting attributo %s to %s", attributo, value)
        self._attributi_table.set_single_manual(attributo, value)
        # if self._current_sample.id is not None:
        #     self._db.update_sample_attributo(
        #         conn, self._current_sample.id, attributo, value
        #     )

    def _slot_refresh_with_conn(self) -> None:
        with self._db.connect() as conn:
            self._slot_refresh(conn)

    def _slot_refresh(self, conn: Connection) -> None:
        # FIXME: What if the current sample was deleted?
        # FIXME: We could reload the sample data here, but that'd impede editing a bit if we don't do it smart
        self._attributi_table.data_changed(
            self._attributi_table.attributi.to_raw(),
            self._db.retrieve_table_attributi(conn, AssociatedTable.SAMPLE),
            sample_ids=[],
        )

        self._reload_and_fill_samples(conn)

    def hideEvent(self, _e: Any) -> None:
        self._update_timer.stop()

    def showEvent(self, _e: Any) -> None:
        if self._auto_refresh.isChecked():
            self._slot_refresh_with_conn()
            self._update_timer.start(AUTO_REFRESH_TIMER_MSEC)

    def _display_micrograph(self) -> None:
        assert self._current_sample.micrograph is not None
        display_image_viewer(Path(self._current_sample.micrograph), parent=self)

    def _open_protocol(self) -> None:
        assert self._current_sample.protocol is not None
        QDesktopServices.openUrl(QUrl(f"file://{self._current_sample.protocol}"))

    def _choose_protocol(self) -> None:
        result, _ = QFileDialog.getOpenFileName(
            self,
            "Choose a protocol file",
        )
        if not result:
            return

        self._protocol_edit.setText(result)
        self._current_sample = replace(self._current_sample, protocol=result)
        self._open_protocol_button.setEnabled(True)

    def _choose_micrograph(self) -> None:
        result, _ = QFileDialog.getOpenFileName(
            self,
            "Choose an image file",
        )
        if not result:
            return

        self._micrograph_edit.setText(result)
        self._current_sample = replace(self._current_sample, micrograph=result)
        self._display_micrograph_button.setEnabled(True)

    def _slot_delete_sample(self) -> None:
        row_idx = self._sample_table.currentRow()
        sample = self._samples[row_idx]
        sample_id = cast(int, sample.id)

        refs = self._samples_with_runs.get(sample_id, [])
        if refs:
            mb = QMessageBox(  # type: ignore
                QMessageBox.Critical,
                f"Cannot delete sample “{sample_id}”",
                "<p>The sample is in used by the following run(s) and cannot be deleted!</p><p>Please reset the "
                "samples for the "
                "runs and try again:</p><ul>"
                + ("".join([f"<li>Run {x}</li>" for x in refs][0:20]))
                + ("<li>...</li>" if len(refs) > 20 else "")
                + "</ul>",
                QMessageBox.Ok,
                self,
            )
            mb.setTextFormat(Qt.RichText)
            mb.exec()
            return

        result = QMessageBox(  # type: ignore
            QMessageBox.Critical,
            f"Delete “{sample_id}”",
            f"Are you sure you want to delete sample “{sample_id}”?",
            QMessageBox.Yes | QMessageBox.Cancel,
            self,
        ).exec()

        if result == QMessageBox.Yes:
            with self._db.connect() as conn:
                self._db.delete_sample(conn, sample_id)
                self._log_widget.setText(f"Sample “{sample_id}” deleted!")
                self._reload_and_fill_samples(conn)
                if self._current_sample.id == sample_id:
                    self._reset_input_fields()
                    self._right_headline.setText(NEW_SAMPLE_HEADLINE)

    def _create_add_button(self):
        b = QPushButton(
            self.style().standardIcon(QStyle.SP_DialogOkButton), "Add sample"
        )
        b.clicked.connect(self._slot_add_sample)
        return b

    def _create_edit_button(self):
        b = QPushButton(
            self.style().standardIcon(QStyle.SP_BrowserReload), "Edit sample"
        )
        b.clicked.connect(self._slot_edit_sample)
        return b

    def _create_cancel_button(self):
        b = QPushButton(
            self.style().standardIcon(QStyle.SP_DialogCancelButton), "Cancel"
        )
        b.clicked.connect(self._cancel_edit)
        return b

    def _slot_row_selected(self, index: QModelIndex) -> None:
        self._attributo_manual_changes.clear()
        self._current_sample = self._samples[index.row()]
        sample_id = self._current_sample.id
        self._right_headline.setText(f"Edit sample “{sample_id}”")
        self._target_id_edit.set_current_value(self._current_sample.target_id)
        self._micrograph_edit.set_value(self._current_sample.micrograph)  # type: ignore
        self._protocol_edit.set_value(self._current_sample.protocol)  # type: ignore
        # noinspection PyTypeChecker
        self._compounds_edit.set_value(self._current_sample.compounds)  # type: ignore
        self._clear_submit()
        self._submit_layout.addWidget(self._create_edit_button())
        self._submit_layout.addWidget(self._create_cancel_button())

        self._attributi_table.data_changed(
            self._current_sample.attributi,
            self._attributi_table.metadata,
            sample_ids=[],
        )

    def _clear_submit(self):
        while True:
            result = self._submit_layout.takeAt(0)
            if result is not None and result.widget() is not None:
                result.widget().hide()
            if result is None:
                break

    def _cancel_edit(self) -> None:
        self._attributo_manual_changes.clear()
        self._clear_submit()
        self._submit_layout.addWidget(self._create_add_button())
        self._reset_input_fields()
        self._right_headline.setText(NEW_SAMPLE_HEADLINE)

    def _slot_add_sample(self) -> None:
        with self._db.connect() as conn:
            self._db.add_sample(
                conn,
                replace(
                    self._current_sample,
                    attributi=self._attributi_table.attributi.to_raw(),
                ),
            )
            self._reset_input_fields()
            self._log_widget.setText("Sample successfully added!")
            self._reload_and_fill_samples(conn)

    def _slot_edit_sample(self) -> None:
        with self._db.connect() as conn:
            self._db.edit_sample(
                conn,
                replace(
                    self._current_sample,
                    attributi=self._attributi_table.attributi.to_raw(),
                ),
            )
            self._log_widget.setText("Sample successfully edited!")
            self._reload_and_fill_samples(conn)

    def _reset_input_fields(self):
        self._compounds_edit.set_value(None)
        self._micrograph_edit.set_value(None)
        self._protocol_edit.set_value(None)
        self._current_sample = _empty_sample(self._attributi_table.metadata)
        self._attributi_table.data_changed(
            self._current_sample.attributi, self._attributi_table.metadata, []
        )

    def _target_id_change(self, new_id: int) -> None:
        self._current_sample = replace(self._current_sample, target_id=new_id)
        self._reset_button()

    def _micrograph_change(self, value: str) -> None:
        if not isinstance(value, Partial):
            self._current_sample = replace(self._current_sample, micrograph=value)
        self._display_micrograph_button.setEnabled(
            self._current_sample.micrograph is not None
        )
        self._reset_button()

    def _protocol_change(self, value: str) -> None:
        if not isinstance(value, Partial):
            self._current_sample = replace(self._current_sample, protocol=value)
        self._open_protocol_button.setEnabled(self._current_sample.protocol is not None)
        self._reset_button()

    def _compounds_change(self, value: Union[str, List[int]]) -> None:
        if not isinstance(value, Partial):
            self._current_sample = replace(self._current_sample, compounds=value)
        self._reset_button()

    def _button_enabled(self) -> bool:
        return self._compounds_edit.valid_value()

    def _reset_button(self) -> None:
        self._add_button.setEnabled(self._button_enabled())

    def _short_name_changed(self, new_name: str) -> None:
        self._current_sample = replace(self._current_sample, short_name=new_name)
        self._reset_button()

    def _name_changed(self, new_name: str) -> None:
        self._current_sample = replace(self._current_sample, name=new_name)
        self._reset_button()

    def _reload_and_fill_samples(self, conn: Connection) -> None:
        self._samples_with_runs = self._db.retrieve_used_sample_ids(conn)
        self._samples = self._db.retrieve_samples(conn)
        self._targets = self._db.retrieve_targets(conn)

        self._target_id_edit.reset_items(
            [("None", cast(Optional[int], None))]
            + [(s.short_name, cast(Optional[int], s.id)) for s in self._targets]
        )
        self._fill_table()

    def _fill_table(self):
        self._sample_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        selected_row = next(iter(self._sample_table.selectedItems()), None)
        selected_row = selected_row.row() if selected_row is not None else None
        self._sample_table.clear()
        attributi_headers = [
            k.pretty_id() for k in self._attributi_table.metadata.values()
        ]
        headers = [
            "ID",
            "Number of runs",
            "Target",
            "Compounds",
        ] + attributi_headers
        self._sample_table.setColumnCount(len(headers))
        self._sample_table.setHorizontalHeaderLabels(headers)
        self._sample_table.setRowCount(len(self._samples))
        self._sample_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._sample_table.verticalHeader().hide()
        for row, sample in enumerate(self._samples):
            built_in_columns = (
                str(sample.id),
                str(len(self._samples_with_runs.get(sample.id, []))),
                next(
                    iter(
                        t.short_name for t in self._targets if sample.target_id == t.id
                    ),
                    None,
                ),
                ", ".join(sample.compounds if sample is not None else [])
                if sample.compounds is not None
                else "",
                # type: ignore
            )
            for col, column_value in enumerate(built_in_columns):
                self._sample_table.setItem(row, col, QTableWidgetItem(column_value))  # type: ignore
            i = len(built_in_columns)
            attributi = AttributiMap(self._attributi_table.metadata, sample.attributi)
            for attributo_id in self._attributi_table.metadata:
                attributo_value = attributi.select(attributo_id)
                self._sample_table.setItem(
                    row,
                    i,
                    QTableWidgetItem(
                        pretty_print_attributo(
                            self._attributi_table.metadata.get(attributo_id, None),
                            attributo_value.value
                            if attributo_value is not None
                            else None,
                        )
                    ),
                )
                i += 1
        self._sample_table.resizeColumnsToContents()
        if selected_row is not None:
            self._sample_table.selectRow(selected_row)
