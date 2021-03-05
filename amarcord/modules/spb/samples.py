import datetime
import getpass
import logging
from dataclasses import replace
from typing import List, Optional, Union, cast

from PyQt5.QtCore import QModelIndex, Qt, pyqtSignal
from PyQt5.QtGui import QContextMenuEvent
from PyQt5.QtWidgets import (
    QAbstractItemView,
    QFormLayout,
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

from amarcord.modules.context import Context
from amarcord.modules.spb.db import DB, DBSample
from amarcord.modules.spb.db_tables import DBTables
from amarcord.numeric_range import NumericRange
from amarcord.qt.combo_box import ComboBox
from amarcord.qt.datetime import parse_natural_delta, print_natural_delta
from amarcord.qt.numeric_input_widget import NumericInputValue, NumericInputWidget
from amarcord.qt.pubchem import validate_pubchem_compound
from amarcord.qt.validated_line_edit import ValidatedLineEdit
from amarcord.qt.validators import (
    parse_date_time,
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
        created=datetime.datetime.utcnow(),
        target_id=-1,
        average_crystal_size=None,
        crystal_shape=None,
        incubation_time=None,
        crystal_buffer=None,
        crystallization_temperature=None,
        protein_concentration=None,
        shaking_time=None,
        shaking_strength=None,
        comment="",
        crystal_settlement_volume=None,
        seed_stock_used="",
        plate_origin="",
        creator=getpass.getuser(),
        crystallization_method="",
        filters=None,
        compounds=None,
    )


def _validate_pubchem(s: str) -> Union[int, str, None]:
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
    def __init__(
        self,
        context: Context,
        tables: DBTables,
        parent: Optional[QWidget] = None,
    ) -> None:
        super().__init__(parent)

        self._db = DB(context.db, tables)
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

        self._target_id_edit = ComboBox[int](items=[], selected=None)
        right_form_layout.addRow(
            "Target",
            self._target_id_edit,
        )
        self._target_id_edit.item_selected.connect(self._target_id_change)

        self._crystal_settlement_volume_edit = NumericInputWidget(
            None,
            NumericRange(
                0.0, minimum_inclusive=True, maximum=100.0, maximum_inclusive=True
            ),
            placeholder="Value in %",
        )
        self._crystal_settlement_volume_edit.value_change.connect(
            self._crystal_settlement_volume_change
        )
        right_form_layout.addRow(
            "Crystal Settlement volume",
            self._crystal_settlement_volume_edit,
        )
        self._shaking_strength_edit = NumericInputWidget(
            None,
            NumericRange(
                0.0, minimum_inclusive=True, maximum=None, maximum_inclusive=False
            ),
            placeholder="Value in RPM",
        )
        self._shaking_strength_edit.value_change.connect(self._shaking_strength_change)
        self._average_crystal_size_edit = NumericInputWidget(
            None,
            NumericRange(
                0.0, minimum_inclusive=True, maximum=None, maximum_inclusive=False
            ),
            placeholder="Value in μm",
        )
        self._average_crystal_size_edit.value_change.connect(
            self._average_crystal_size_change
        )
        right_form_layout.addRow(
            "Average crystal size",
            self._average_crystal_size_edit,
        )
        self._crystallization_temperature_edit = NumericInputWidget(
            None,
            NumericRange(
                None, minimum_inclusive=True, maximum=None, maximum_inclusive=False
            ),
            placeholder="Value in °C",
        )
        self._crystallization_temperature_edit.value_change.connect(
            self._crystallization_temperature_change
        )
        right_form_layout.addRow(
            "Crystallization Temperature",
            self._crystallization_temperature_edit,
        )
        self._protein_concentration_edit = NumericInputWidget(
            None,
            NumericRange(
                0.0, minimum_inclusive=True, maximum=None, maximum_inclusive=False
            ),
            placeholder="Value in mg/mL",
        )
        self._protein_concentration_edit.value_change.connect(
            self._protein_concentration_change
        )
        right_form_layout.addRow(
            "Protein concentration",
            self._protein_concentration_edit,
        )
        self._shaking_time_edit = ValidatedLineEdit(
            None,
            print_natural_delta,
            parse_natural_delta,
            "example: 1 day, 5 hours, 30 minutes",
        )
        self._shaking_time_edit.value_change.connect(self._shaking_time_change)
        right_form_layout.addRow(
            "Shaking Strength",
            self._shaking_strength_edit,
        )
        right_form_layout.addRow(
            "Shaking time",
            self._shaking_time_edit,
        )
        self._comment_edit = QLineEdit()
        right_form_layout.addRow(
            "Comment",
            self._comment_edit,
        )
        self._comment_edit.textEdited.connect(self._comment_edit_change)
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
        self._plate_origin_edit = QLineEdit()
        right_form_layout.addRow(
            "Plate Origin",
            self._plate_origin_edit,
        )
        self._plate_origin_edit.textEdited.connect(self._plate_origin_edit_change)
        self._seed_stock_used_edit = QLineEdit()
        right_form_layout.addRow(
            "Seed Stock Used",
            self._seed_stock_used_edit,
        )
        self._seed_stock_used_edit.textEdited.connect(self._seed_stock_used_edit_change)
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
        self._crystal_buffer_edit = QLineEdit()
        self._crystal_buffer_edit.textEdited.connect(self._crystal_buffer_change)
        right_form_layout.addRow("Crystal Buffer", self._crystal_buffer_edit)
        self._incubation_time_edit.value_change.connect(self._incubation_time_change)
        self._submit_widget = QWidget()
        self._submit_layout = QHBoxLayout()
        self._submit_layout.setContentsMargins(0, 0, 0, 0)
        self._submit_widget.setLayout(self._submit_layout)
        self._add_button = self._create_add_button()
        self._add_button.setEnabled(True)
        self._submit_layout.addWidget(self._add_button)
        right_form_layout.addWidget(self._submit_widget)

        self._samples: List[DBSample] = []
        self._fill_table()

    def _delete_sample(self) -> None:
        row_idx = self._sample_table.currentRow()
        sample = self._samples[row_idx]

        assert sample.id is not None

        result = QMessageBox(  # type: ignore
            QMessageBox.Critical,
            f"Delete “{sample.id}”",
            f"Are you sure you want to delete sample “{sample.id}”?",
            QMessageBox.Yes | QMessageBox.Cancel,
            self,
        ).exec()

        if result == QMessageBox.Yes:
            with self._db.connect() as conn:
                self._db.delete_sample(conn, sample.id)
                self._log_widget.setText(f"Sample “{sample.id}” deleted!")
                self._fill_table()
                if self._current_sample.id == sample.id:
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
        self._current_sample = self._samples[index.row()]
        self._right_headline.setText(f"Edit sample “{self._current_sample.id}”")
        self._comment_edit.setText(self._current_sample.comment)
        self._seed_stock_used_edit.setText(self._current_sample.seed_stock_used)
        self._plate_origin_edit.setText(self._current_sample.plate_origin)
        self._target_id_edit.set_current_value(self._current_sample.target_id)
        self._creator_edit.setText(self._current_sample.creator)
        self._crystallization_method_edit.setText(
            self._current_sample.crystallization_method
        )
        self._average_crystal_size_edit.set_value(
            self._current_sample.average_crystal_size
        )
        self._crystal_settlement_volume_edit.set_value(
            self._current_sample.crystal_settlement_volume
        )
        self._crystallization_temperature_edit.set_value(
            self._current_sample.crystallization_temperature
        )
        self._shaking_time_edit.set_value(
            self._current_sample.shaking_time
            if self._current_sample.shaking_time is not None
            else None
        )
        self._shaking_strength_edit.set_value(self._current_sample.shaking_strength)
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
        self._crystal_buffer_edit.setText(
            self._current_sample.crystal_buffer
            if self._current_sample.crystal_buffer
            else ""
        )
        self._clear_submit()
        self._submit_layout.addWidget(self._create_edit_button())
        self._submit_layout.addWidget(self._create_cancel_button())

    def _clear_submit(self):
        while True:
            result = self._submit_layout.takeAt(0)
            if result is not None and result.widget() is not None:
                result.widget().hide()
            if result is None:
                break

    def _cancel_edit(self) -> None:
        self._clear_submit()
        self._submit_layout.addWidget(self._create_add_button())
        self._reset_input_fields()
        self._right_headline.setText(NEW_SAMPLE_HEADLINE)

    def _add_sample(self) -> None:
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
        self._average_crystal_size_edit.set_value(None)
        self._comment_edit.setText("")
        self._creator.setText("")
        self._seed_stock_used_edit.setText("")
        self._plate_origin_edit.setText("")
        self._crystallization_method_edit.setText("")
        self._shaking_strength_edit.set_value(None)
        self._shaking_time_edit.set_value(None)
        self._crystallization_temperature_edit.set_value(None)
        self._crystal_shape_edit.set_value(None)
        self._filters_edit.set_value(None)
        self._compounds_edit.set_value(None)
        self._incubation_time_edit.set_value(None)
        self._crystal_settlement_volume_edit.set_value(None)
        self._crystal_buffer_edit.setText("")
        self._current_sample = _empty_sample()
        self._protein_concentration_edit.set_value(None)

    def _target_id_change(self, new_id: int) -> None:
        self._current_sample = replace(self._current_sample, target_id=new_id)
        self._reset_button()

    def _comment_edit_change(self, new_comment: str) -> None:
        self._current_sample = replace(self._current_sample, comment=new_comment)
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

    def _plate_origin_edit_change(self, new_plate_origin: str) -> None:
        self._current_sample = replace(
            self._current_sample, plate_origin=new_plate_origin
        )
        self._reset_button()

    def _seed_stock_used_edit_change(self, new_seed_stock_used: str) -> None:
        self._current_sample = replace(
            self._current_sample, seed_stock_used=new_seed_stock_used
        )
        self._reset_button()

    def _shaking_time_change(self, value: Union[str, datetime.timedelta]) -> None:
        if not isinstance(value, str):
            self._current_sample = replace(self._current_sample, shaking_time=value)
        self._reset_button()

    def _crystal_shape_change(self, value: Union[str, List[float]]) -> None:
        if not isinstance(value, str):
            self._current_sample = replace(self._current_sample, crystal_shape=value)
        self._reset_button()

    def _filters_change(self, value: Union[str, List[str]]) -> None:
        if not isinstance(value, str):
            self._current_sample = replace(self._current_sample, filters=value)
        self._reset_button()

    def _compounds_change(self, value: Union[str, List[int]]) -> None:
        if not isinstance(value, str):
            self._current_sample = replace(self._current_sample, compounds=value)
        self._reset_button()

    def _crystal_buffer_change(self, value: str) -> None:
        self._current_sample = replace(self._current_sample, crystal_buffer=value)
        self._reset_button()

    def _incubation_time_change(self, value: Union[str, datetime.datetime]) -> None:
        if not isinstance(value, str):
            self._current_sample = replace(self._current_sample, incubation_time=value)
        self._reset_button()

    def _crystallization_temperature_change(self, value: NumericInputValue) -> None:
        if not isinstance(value, str):
            self._current_sample = replace(
                self._current_sample, crystallization_temperature=value
            )
        self._reset_button()

    def _protein_concentration_change(self, value: NumericInputValue) -> None:
        if not isinstance(value, str):
            self._current_sample = replace(
                self._current_sample, protein_concentration=value
            )
        self._reset_button()

    def _crystal_settlement_volume_change(self, value: NumericInputValue) -> None:
        if not isinstance(value, str):
            self._current_sample = replace(
                self._current_sample, crystal_settlement_volume=value
            )
        self._reset_button()

    def _shaking_strength_change(self, value: NumericInputValue) -> None:
        if not isinstance(value, str):
            self._current_sample = replace(self._current_sample, shaking_strength=value)
        self._reset_button()

    def _average_crystal_size_change(self, value: NumericInputValue) -> None:
        if not isinstance(value, str):
            self._current_sample = replace(
                self._current_sample, average_crystal_size=value
            )
        self._reset_button()

    def _button_enabled(self) -> bool:
        return (
            self._average_crystal_size_edit.valid_value()
            and self._crystal_shape_edit.valid_value()
            and self._incubation_time_edit.valid_value()
            and self._crystallization_temperature_edit.valid_value()
            and self._shaking_time_edit.valid_value()
            and self._shaking_strength_edit.valid_value()
            and self._protein_concentration_edit.valid_value()
            and self._crystal_settlement_volume_edit.valid_value()
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

    def _fill_table(self) -> None:
        with self._db.connect() as conn:
            self._samples = self._db.retrieve_samples(conn)
            self._targets = self._db.retrieve_targets(conn)

        self._target_id_edit.reset_items(
            [(s.short_name, cast(int, s.id)) for s in self._targets]
        )
        self._sample_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._sample_table.clear()
        headers = [
            "ID",
            "Created",
            "Incubation Time",
            "Crystal Buffer",
            "Crystallization Temperature",
            "Avg Crystal Size",
            "Crystal Shape",
            "Target",
            "Shaking Time",
            "Shaking Strength",
            "Protein Concentration",
            "Crystal Settlement Volume",
            "Seed Stock Used",
            "Plate Origin",
            "Creator",
            "Crystallization Method",
            "Filters",
            "Compounds",
        ]
        self._sample_table.setColumnCount(len(headers))
        self._sample_table.setHorizontalHeaderLabels(headers)
        self._sample_table.setRowCount(len(self._samples))
        self._sample_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._sample_table.verticalHeader().hide()

        for row, sample in enumerate(self._samples):
            for col, column_value in enumerate(
                (
                    str(sample.id),
                    str(sample.created),
                    sample.incubation_time.strftime(DATE_TIME_FORMAT)
                    if sample.incubation_time is not None
                    else "",
                    sample.crystal_buffer if sample.crystal_buffer is not None else "",
                    str(sample.crystallization_temperature)
                    if sample.crystallization_temperature is not None
                    else "",
                    str(sample.average_crystal_size),
                    ", ".join(str(s) for s in sample.crystal_shape)
                    if sample.crystal_shape is not None
                    else "",
                    [t.short_name for t in self._targets if sample.target_id == t.id][
                        0
                    ],
                    print_natural_delta(sample.shaking_time)
                    if sample.shaking_time is not None
                    else "",
                    sample.shaking_strength
                    if sample.shaking_strength is not None
                    else "",
                    str(sample.protein_concentration)
                    if sample.protein_concentration is not None
                    else "",
                    sample.comment,
                    str(sample.crystal_settlement_volume)
                    if sample.crystal_settlement_volume
                    else "",
                    sample.seed_stock_used,
                    sample.plate_origin,
                    sample.creator,
                    sample.crystallization_method,
                    ", ".join(sample.filters) if sample.filters is not None else "",
                    ", ".join(sample.compounds) if sample.compounds is not None else "",
                )
            ):
                self._sample_table.setItem(row, col, QTableWidgetItem(column_value))

        self._sample_table.resizeColumnsToContents()
