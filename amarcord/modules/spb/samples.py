import logging
import urllib.request
from dataclasses import replace
from typing import List, Optional, Union

from PyQt5.QtCore import QModelIndex, Qt, pyqtSignal
from PyQt5.QtGui import QContextMenuEvent
from PyQt5.QtWidgets import (
    QAbstractItemView,
    QFormLayout,
    QHBoxLayout,
    QLabel,
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
from amarcord.qt.number_list_validator import NumberListValidator, parse_float_list
from amarcord.qt.numeric_input_widget import NumericInputValue, NumericInputWidget
from amarcord.qt.validated_line_edit import ValidatedLineEdit

NEW_SAMPLE_HEADLINE = "New sample"

logger = logging.getLogger(__name__)


def _empty_sample():
    return DBSample(
        id=None, target_id=-1, average_crystal_size=None, crystal_shape=None
    )


def _validate_uniprot(uniprot_id: str) -> bool:
    try:
        with urllib.request.urlopen(
            f"https://www.uniprot.org/uniprot/{uniprot_id}.fasta"
        ):
            return True
    except:
        return False


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
        self._crystal_shape_edit = ValidatedLineEdit(
            None,
            NumberListValidator(3),
            lambda float_list: ", ".join(str(s) for s in float_list),  # type: ignore
            lambda float_list_str: parse_float_list(float_list_str, 3),  # type: ignore
            "a, b, c separated by commas",
        )
        self._crystal_shape_edit.value_change.connect(self._crystal_shape_change)
        right_form_layout.addRow("Crystal shape", self._crystal_shape_edit)
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
        self._average_crystal_size_edit.set_value(
            self._current_sample.average_crystal_size
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
            self._reset_input_fields()
            self._log_widget.setText("Sample successfully edited!")
            self._right_headline.setText(NEW_SAMPLE_HEADLINE)
            self._fill_table()

    def _reset_input_fields(self):
        self._average_crystal_size_edit.set_value(None)
        self._current_sample = _empty_sample()

    def _crystal_shape_change(self, value: Union[str, List[float]]) -> None:
        if not isinstance(value, str):
            self._current_sample = replace(self._current_sample, crystal_shape=value)
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

        self._sample_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._sample_table.clear()
        self._sample_table.setColumnCount(3)
        self._sample_table.setRowCount(len(self._samples))
        self._sample_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._sample_table.setHorizontalHeaderLabels(
            ["ID", "Avg Crystal Size", "Target"]
        )
        self._sample_table.verticalHeader().hide()

        for row, sample in enumerate(self._samples):
            for col, column_value in enumerate(
                (
                    str(sample.id),
                    str(sample.average_crystal_size),
                    [t.short_name for t in self._targets if sample.target_id == t.id][
                        0
                    ],
                )
            ):
                self._sample_table.setItem(row, col, QTableWidgetItem(column_value))

        self._sample_table.resizeColumnsToContents()
