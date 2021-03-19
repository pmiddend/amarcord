import logging
from dataclasses import replace
from typing import List, Optional

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
from amarcord.db.db import DB, DBTarget
from amarcord.db.tables import DBTables
from amarcord.numeric_range import NumericRange
from amarcord.qt.debounced_line_edit import DebouncedLineEdit
from amarcord.qt.numeric_input_widget import NumericInputValue, NumericInputWidget
from amarcord.uniprot import validate_uniprot

NEW_TARGET_HEADLINE = "New target"

logger = logging.getLogger(__name__)


def _empty_target():
    return DBTarget(
        id=None, name="", short_name="", molecular_weight=None, uniprot_id=""
    )


class _TargetTable(QTableWidget):
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


class Targets(QWidget):
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
        self._target_table = _TargetTable()
        self._target_table.delete_current_row.connect(self._delete_target)
        self._target_table.doubleClicked.connect(self._slot_row_selected)
        root_widget.addWidget(self._target_table)

        self._current_target = _empty_target()
        right_widget = QWidget()
        right_root_layout = QVBoxLayout()
        right_widget.setLayout(right_root_layout)
        self._right_headline = QLabel(NEW_TARGET_HEADLINE)
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
        self._uniprot_edit = DebouncedLineEdit()

        self._short_name_edit = QLineEdit()
        self._short_name_edit.textEdited.connect(self._short_name_changed)
        right_form_layout.addRow("Short Name*", self._short_name_edit)
        self._name_edit = QLineEdit()
        self._name_edit.textEdited.connect(self._name_changed)
        right_form_layout.addRow("Name*", self._name_edit)
        self._molecular_weight_edit = NumericInputWidget(
            None,
            NumericRange(
                0.0, minimum_inclusive=True, maximum=None, maximum_inclusive=False
            ),
            placeholder="Value in g/mol",
        )
        self._molecular_weight_edit.value_change.connect(self._molecular_weight_change)
        self._molecular_weight: NumericInputValue = None
        right_form_layout.addRow(
            "Molecular weight",
            self._molecular_weight_edit,
        )
        self._uniprot_edit.debouncedTextChanged.connect(self._uniprot_change)
        right_form_layout.addRow("Uniprot ID", self._uniprot_edit)
        self._submit_widget = QWidget()
        self._submit_layout = QHBoxLayout()
        self._submit_layout.setContentsMargins(0, 0, 0, 0)
        self._submit_widget.setLayout(self._submit_layout)
        self._edit_button: Optional[QPushButton] = None
        self._add_button = self._create_add_button()
        self._add_button.setEnabled(False)
        self._submit_layout.addWidget(self._add_button)
        right_form_layout.addWidget(self._submit_widget)

        self._targets: List[DBTarget] = []
        self._fill_table()

    def _delete_target(self) -> None:
        row_idx = self._target_table.currentRow()
        target = self._targets[row_idx]

        assert target.id is not None

        result = QMessageBox.question(
            self,
            f"Delete “{target.short_name}”",
            f"Are you sure you want to delete “{target.short_name}”?",
        )

        if result == QMessageBox.Yes:
            with self._db.connect() as conn:
                with conn.begin():
                    samples_used = [
                        str(s.id)
                        for s in self._db.retrieve_samples(conn)
                        if s.target_id == target.id
                    ]
                    if samples_used:
                        QMessageBox.critical(
                            self,
                            "Cannot delete",
                            "The target is used by the following samples:\n\n"
                            + "\n".join(samples_used),
                        )
                        return
                    self._db.delete_target(conn, target.id)
                    self._log_widget.setText(f"“{target.short_name}” deleted!")
                    self._fill_table()
                    if self._current_target.id == target.id:
                        self._reset_input_fields()
                        self._right_headline.setText(NEW_TARGET_HEADLINE)

    def _create_add_button(self) -> QPushButton:
        b = QPushButton(
            self.style().standardIcon(QStyle.SP_DialogOkButton), "Add target"
        )
        b.clicked.connect(self._add_target)
        return b

    def _create_edit_button(self) -> QPushButton:
        b = QPushButton(
            self.style().standardIcon(QStyle.SP_BrowserReload), "Edit target"
        )
        b.clicked.connect(self._edit_target)
        return b

    def _create_cancel_button(self) -> QPushButton:
        b = QPushButton(
            self.style().standardIcon(QStyle.SP_DialogCancelButton), "Cancel"
        )
        b.clicked.connect(self._cancel_edit)
        return b

    def _slot_row_selected(self, index: QModelIndex) -> None:
        self._current_target = self._targets[index.row()]
        self._right_headline.setText(f"Edit “{self._current_target.short_name}”")
        self._name_edit.setText(self._current_target.name)
        self._short_name_edit.setText(self._current_target.short_name)
        self._molecular_weight_edit.set_value(self._current_target.molecular_weight)
        self._uniprot_edit.setText(self._current_target.uniprot_id)
        self._molecular_weight = self._current_target.molecular_weight
        self._clear_submit()
        self._edit_button = self._create_edit_button()
        self._submit_layout.addWidget(self._edit_button)
        self._submit_layout.addWidget(self._create_cancel_button())
        self._reset_button()

    def _clear_submit(self):
        while True:
            result = self._submit_layout.takeAt(0)
            if result is not None and result.widget() is not None:
                result.widget().hide()
            if result is None:
                break

        self._add_button = None
        self._edit_button = None

    def _cancel_edit(self) -> None:
        self._clear_submit()
        self._add_button = self._create_add_button()
        self._submit_layout.addWidget(self._add_button)
        self._reset_input_fields()
        self._reset_button()
        self._right_headline.setText(NEW_TARGET_HEADLINE)

    def _uniprot_change(self, new_uniprot: str) -> None:
        self._uniprot_edit.setStyleSheet(
            "background-color: #ffb8b8"
            if new_uniprot != "" and not validate_uniprot(new_uniprot)
            else ""
        )
        self._current_target = replace(self._current_target, uniprot_id=new_uniprot)

    def _add_target(self) -> None:
        with self._db.connect() as conn:
            self._db.add_target(conn, self._current_target)
            self._reset_input_fields()
            self._log_widget.setText("Target successfully added!")
            self._fill_table()

    def _edit_target(self) -> None:
        with self._db.connect() as conn:
            self._db.edit_target(conn, self._current_target)
            self._log_widget.setText("Target successfully edited!")
            self._fill_table()

    def _reset_input_fields(self):
        self._name_edit.setText("")
        self._short_name_edit.setText("")
        self._uniprot_edit.setText("")
        self._molecular_weight_edit.set_value(None)
        self._current_target = _empty_target()

    def _molecular_weight_change(self, value: NumericInputValue) -> None:
        self._molecular_weight = value
        if not isinstance(value, str):
            self._current_target = replace(self._current_target, molecular_weight=value)
        self._reset_button()

    def _submit_button_enabled(self) -> bool:
        return (
            bool(self._current_target.short_name)
            and bool(self._current_target.name)
            and self._molecular_weight_edit.valid_value()
            and (
                self._current_target.id is not None
                or self._current_target.short_name
                not in [t.short_name for t in self._targets]
            )
        )

    def _reset_button(self) -> None:
        if self._add_button is not None:
            self._add_button.setEnabled(self._submit_button_enabled())
        else:
            assert self._edit_button is not None
            self._edit_button.setEnabled(self._submit_button_enabled())

    def _short_name_changed(self, new_name: str) -> None:
        self._current_target = replace(self._current_target, short_name=new_name)
        self._reset_button()

    def _name_changed(self, new_name: str) -> None:
        self._current_target = replace(self._current_target, name=new_name)
        self._reset_button()

    def _fill_table(self) -> None:
        with self._db.connect() as conn:
            self._targets = self._db.retrieve_targets(conn)

        self._target_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._target_table.clear()
        self._target_table.setColumnCount(4)
        self._target_table.setRowCount(len(self._targets))
        self._target_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._target_table.setHorizontalHeaderLabels(
            ["Short name", "Name", "Molecular weight", "Uniprot ID"]
        )
        self._target_table.verticalHeader().hide()

        for row, t in enumerate(self._targets):
            for col, s in enumerate(
                (
                    t.short_name,
                    t.name,
                    str(t.molecular_weight) if t.molecular_weight is not None else None,
                    t.uniprot_id,
                )
            ):
                self._target_table.setItem(
                    row, col, QTableWidgetItem(s if s is not None else "")
                )

        self._target_table.resizeColumnsToContents()
