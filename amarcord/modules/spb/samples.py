import datetime
import getpass
import logging
from dataclasses import replace
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, cast

from PyQt5.QtCore import QModelIndex, QUrl, Qt, pyqtSignal
from PyQt5.QtGui import QContextMenuEvent, QDesktopServices
from PyQt5.QtWidgets import (
    QAbstractItemView,
    QFileDialog,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMenu,
    QMessageBox,
    QPushButton,
    QSplitter,
    QStyle,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributi import (
    AttributiMap,
    pretty_print_attributo,
)
from amarcord.db.attributo_id import AttributoId
from amarcord.db.constants import MANUAL_SOURCE_NAME
from amarcord.db.db import Connection, DB, DBSample
from amarcord.db.raw_attributi_map import RawAttributiMap
from amarcord.db.tables import DBTables
from amarcord.modules.context import Context
from amarcord.modules.spb.attributi_table import AttributiTable
from amarcord.qt.combo_box import ComboBox
from amarcord.qt.datetime import parse_natural_delta, print_natural_delta
from amarcord.qt.image_viewer import display_image_viewer
from amarcord.qt.pubchem import validate_pubchem_compound
from amarcord.qt.validated_line_edit import ValidatedLineEdit
from amarcord.qt.validators import (
    Partial,
    parse_date_time,
    parse_existing_filename,
    parse_float_list,
    parse_list,
    parse_string_list,
)
from amarcord.util import str_to_int

DATE_TIME_FORMAT = "%Y-%m-%d %H:%M"

NEW_SAMPLE_HEADLINE = "New sample"

logger = logging.getLogger(__name__)


def _empty_sample():
    return DBSample(
        id=None,
        target_id=-1,
        crystal_shape=None,
        incubation_time=None,
        creator=getpass.getuser(),
        crystallization_method="",
        filters=None,
        compounds=None,
        micrograph=None,
        protocol=None,
        attributi=RawAttributiMap(
            {
                MANUAL_SOURCE_NAME: {
                    AttributoId("created"): datetime.datetime.utcnow().isoformat(),
                }
            }
        ),
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

        if "filesystem" not in context.config:
            raise Exception('Cannot find "filesystem" in configuration')
        if "base_path" not in context.config["filesystem"]:
            raise Exception('Cannot find "filesystem.base_path" in configuration')
        if not isinstance(context.config["filesystem"]["base_path"], str):
            raise Exception('"filesystem.base_path" in configuration is not a string')
        self._proposal_file_path = Path(context.config["filesystem"]["base_path"])
        root_layout = QHBoxLayout()
        self.setLayout(root_layout)

        root_widget = QSplitter(self)
        root_layout.addWidget(root_widget)
        self._sample_table = _SampleTable()
        self._sample_table.delete_current_row.connect(self._delete_sample)
        self._sample_table.doubleClicked.connect(self._slot_row_selected)
        root_widget.addWidget(self._sample_table)

        self._current_sample = _empty_sample()
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
        right_root_layout.addStretch()
        right_widget.setLayout(right_root_layout)
        root_widget.addWidget(right_widget)

        self._target_id_edit = ComboBox[int](items=[(str(-1), -1)], selected=-1)
        right_form_layout.addRow(
            "Target",
            self._target_id_edit,
        )
        self._target_id_edit.item_selected.connect(self._target_id_change)

        self._creator_edit = QLineEdit(getpass.getuser())
        right_form_layout.addRow(
            "Creator",
            self._creator_edit,
        )
        self._creator_edit.textEdited.connect(self._creator_edit_change)
        self._crystallization_method_edit = QLineEdit()
        right_form_layout.addRow(
            "Crystallization Method",
            self._crystallization_method_edit,
        )
        self._crystallization_method_edit.textEdited.connect(
            self._crystallization_method_edit_change
        )
        self._crystal_shape_edit = ValidatedLineEdit(
            None,
            lambda float_list: ", ".join(str(s) for s in float_list),  # type: ignore
            lambda float_list_str: parse_float_list(float_list_str, 3),  # type: ignore
            "a, b, c separated by commas",
        )
        self._crystal_shape_edit.value_change.connect(self._crystal_shape_change)
        right_form_layout.addRow("Crystal shape", self._crystal_shape_edit)

        self._filters_edit = ValidatedLineEdit(
            None,
            lambda str_list: ", ".join(str(s) for s in str_list),  # type: ignore
            lambda str_list_str: parse_string_list(str_list_str, None),  # type: ignore
            "list of filters, separated by commas",
        )
        self._filters_edit.value_change.connect(self._filters_change)
        right_form_layout.addRow("Filters", self._filters_edit)

        self._compounds_edit = ValidatedLineEdit(
            None,
            lambda str_list: ", ".join(str(s) for s in str_list),  # type: ignore
            lambda str_list_str: parse_list(str_list_str, None, _validate_pubchem),  # type: ignore
            "list of pubchem CIDs, separated by commas",
        )
        self._compounds_edit.value_change.connect(self._compounds_change)
        right_form_layout.addRow("Compounds", self._compounds_edit)

        self._incubation_time_edit = ValidatedLineEdit(
            None,
            lambda pydatetime: pydatetime.strftime(DATE_TIME_FORMAT),  # type: ignore
            lambda datetimestr: parse_date_time(datetimestr, DATE_TIME_FORMAT),  # type: ignore
            "2020-02-24 15:34",
        )
        right_form_layout.addRow("Incubation time", self._incubation_time_edit)
        self._incubation_time_edit.value_change.connect(self._incubation_time_change)

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
            metadata_wrapper = QGroupBox()
            metadata_wrapper_layout = QHBoxLayout(metadata_wrapper)
            self._attributi_table = AttributiTable(self._attributo_change)
            metadata_wrapper_layout.addWidget(self._attributi_table)
            self._attributi_table.data_changed(
                self._current_sample.attributi,
                self._db.retrieve_table_attributi(conn, AssociatedTable.SAMPLE),
                [],
            )
            attributo_button = QPushButton(
                self.style().standardIcon(QStyle.SP_FileDialogNewFolder),
                "New attributo",
            )
            attributo_button.clicked.connect(self.new_attributo.emit)
            metadata_wrapper_layout.addWidget(attributo_button)
            right_form_layout.addWidget(metadata_wrapper)

        self._submit_widget = QWidget()
        self._submit_layout = QHBoxLayout()
        self._submit_layout.setContentsMargins(0, 0, 0, 0)
        self._submit_widget.setLayout(self._submit_layout)
        self._add_button = self._create_add_button()
        self._add_button.setEnabled(True)
        self._submit_layout.addWidget(self._add_button)

        self._attributo_manual_changes: Dict[AttributoId, Any] = {}
        right_form_layout.addWidget(self._submit_widget)

        self._samples: List[DBSample] = []
        self._fill_table()

    def _attributo_change(self, attributo: AttributoId, value: Any) -> None:
        with self._db.connect() as conn:
            logger.info("Setting attributo %s to %s", attributo, value)
            self._current_sample.attributi.set_single_manual(attributo, value)
            # We could immediately change the attribute, but we have this "Save changes" button
            # if self._current_sample.id is not None:
            #     self._db.update_sample_attributo(
            #         conn, self._current_sample.id, attributo, value
            #     )
            self._slot_refresh(conn)

    def _slot_refresh(self, conn: Connection) -> None:
        self._attributi_table.data_changed(
            self._current_sample.attributi,
            self._db.retrieve_table_attributi(conn, AssociatedTable.SAMPLE),
            sample_ids=[],
        )
        self._fill_table()

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
            str(self._proposal_file_path),
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
            str(self._proposal_file_path),
        )
        if not result:
            return

        self._micrograph_edit.setText(result)
        self._current_sample = replace(self._current_sample, micrograph=result)
        self._display_micrograph_button.setEnabled(True)

    def _delete_sample(self) -> None:
        row_idx = self._sample_table.currentRow()
        sample = self._samples[row_idx]
        sample_id = sample.attributi.select_int_unsafe(AttributoId("id"))

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
                self._fill_table()
                if (
                    self._current_sample.attributi.select_int_unsafe(AttributoId("id"))
                    == sample_id
                ):
                    self._reset_input_fields()
                    self._right_headline.setText(NEW_SAMPLE_HEADLINE)

    def _create_add_button(self):
        b = QPushButton(
            self.style().standardIcon(QStyle.SP_DialogOkButton), "Add sample"
        )
        b.clicked.connect(self._add_sample)
        return b

    def _create_edit_button(self):
        b = QPushButton(
            self.style().standardIcon(QStyle.SP_BrowserReload), "Edit sample"
        )
        b.clicked.connect(self._edit_sample)
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
        self._creator_edit.setText(self._current_sample.creator)
        self._crystallization_method_edit.setText(
            self._current_sample.crystallization_method
        )
        self._micrograph_edit.set_value(self._current_sample.micrograph)  # type: ignore
        self._protocol_edit.set_value(self._current_sample.protocol)  # type: ignore
        # noinspection PyTypeChecker
        self._crystal_shape_edit.set_value(self._current_sample.crystal_shape)  # type: ignore
        # noinspection PyTypeChecker
        self._filters_edit.set_value(self._current_sample.filters)  # type: ignore
        # noinspection PyTypeChecker
        self._compounds_edit.set_value(self._current_sample.compounds)  # type: ignore
        self._incubation_time_edit.setText(
            self._current_sample.incubation_time.strftime(DATE_TIME_FORMAT)
            if self._current_sample.incubation_time is not None
            else ""
        )
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

    def _add_sample(self) -> None:
        assert self._current_sample.target_id > 0
        with self._db.connect() as conn:
            self._db.add_sample(conn, self._current_sample)
            self._reset_input_fields()
            self._log_widget.setText("Sample successfully added!")
            self._fill_table()

    def _edit_sample(self) -> None:
        with self._db.connect() as conn:
            self._db.edit_sample(conn, self._current_sample)
            self._log_widget.setText("Sample successfully edited!")
            self._fill_table()

    def _reset_input_fields(self):
        self._creator_edit.setText("")
        self._crystallization_method_edit.setText("")
        self._crystal_shape_edit.set_value(None)
        self._filters_edit.set_value(None)
        self._compounds_edit.set_value(None)
        self._micrograph_edit.set_value(None)
        self._protocol_edit.set_value(None)
        self._incubation_time_edit.set_value(None)
        self._current_sample = _empty_sample()

    def _target_id_change(self, new_id: int) -> None:
        self._current_sample = replace(self._current_sample, target_id=new_id)
        self._reset_button()

    def _creator_edit_change(self, new_creator: str) -> None:
        self._current_sample = replace(self._current_sample, creator=new_creator)
        self._reset_button()

    def _crystallization_method_edit_change(
        self, new_crystallization_method: str
    ) -> None:
        self._current_sample = replace(
            self._current_sample, crystallization_method=new_crystallization_method
        )
        self._reset_button()

    def _crystal_shape_change(self, value: Union[str, List[float]]) -> None:
        if not isinstance(value, Partial):
            self._current_sample = replace(self._current_sample, crystal_shape=value)
        self._reset_button()

    def _filters_change(self, value: Union[str, List[str]]) -> None:
        if not isinstance(value, Partial):
            self._current_sample = replace(self._current_sample, filters=value)
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

    def _incubation_time_change(self, value: Union[str, datetime.datetime]) -> None:
        if not isinstance(value, Partial):
            self._current_sample = replace(self._current_sample, incubation_time=value)
        self._reset_button()

    def _button_enabled(self) -> bool:
        return (
            self._crystal_shape_edit.valid_value()
            and self._incubation_time_edit.valid_value()
            and self._filters_edit.valid_value()
            and self._compounds_edit.valid_value()
        )

    def _reset_button(self) -> None:
        self._add_button.setEnabled(self._button_enabled())

    def _short_name_changed(self, new_name: str) -> None:
        self._current_sample = replace(self._current_sample, short_name=new_name)
        self._reset_button()

    def _name_changed(self, new_name: str) -> None:
        self._current_sample = replace(self._current_sample, name=new_name)
        self._reset_button()

    def _fill_table(self, conn: Optional[Connection] = None) -> None:
        if conn is not None:
            self._samples = self._db.retrieve_samples(conn)
            self._targets = self._db.retrieve_targets(conn)
        else:
            with self._db.connect() as conn_:
                self._samples = self._db.retrieve_samples(conn_)
                self._targets = self._db.retrieve_targets(conn_)

        self._target_id_edit.reset_items(
            [(s.short_name, cast(int, s.id)) for s in self._targets]
        )
        if self._target_id_edit.current_value() is None and self._targets:
            self._target_id_edit.set_current_value(cast(int, self._targets[0].id))
        self._sample_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._sample_table.clear()
        attributi_headers = [
            k.description for k in self._attributi_table.metadata.values()
        ]
        headers = [
            "ID",
            "Incubation Time",
            "Crystal Shape",
            "Target",
            "Creator",
            "Crystallization Method",
            "Filters",
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
                sample.incubation_time.strftime(DATE_TIME_FORMAT)
                if sample.incubation_time is not None
                else "",
                ", ".join(str(s) for s in sample.crystal_shape)
                if sample.crystal_shape is not None
                else "",
                [t.short_name for t in self._targets if sample.target_id == t.id][0],
                sample.creator,
                sample.crystallization_method,
                ", ".join(sample.filters) if sample.filters is not None else "",
                ", ".join(sample.compounds if sample is not None else []) if sample.compounds is not None else "",  # type: ignore
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
