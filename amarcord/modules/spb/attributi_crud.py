import logging
from enum import Enum
from functools import partial
from typing import Any, Dict, Optional, cast

from PyQt5.QtCore import QPoint, Qt
from PyQt5.QtWidgets import (
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMenu,
    QMessageBox,
    QPushButton,
    QSplitter,
    QStyle,
    QVBoxLayout,
    QWidget,
)

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributi import (
    PropertyDouble,
    PropertyInt,
    RichAttributoType,
    attributo_type_to_string,
)
from amarcord.db.attributo_id import AttributoId
from amarcord.db.db import Connection, DB
from amarcord.db.tabled_attributo import TabledAttributo
from amarcord.db.tables import DBTables
from amarcord.modules.context import Context
from amarcord.modules.spb.new_attributo_dialog import new_attributo_dialog
from amarcord.numeric_range import NumericRange
from amarcord.qt.combo_box import ComboBox
from amarcord.qt.declarative_table import Column, Data, DeclarativeTable, Row
from amarcord.qt.numeric_range_format_widget import NumericRangeFormatWidget

DATE_TIME_FORMAT = "%Y-%m-%d %H:%M"

NEW_SAMPLE_HEADLINE = "New attributo"

logger = logging.getLogger(__name__)


class TypePreset(Enum):
    INT = "integer"
    CHOICE = "choice"
    DOUBLE = "number"
    TAGS = "list of tags"
    STRING = "string"
    DATE_TIME = "date and time"


def _fill_preset(w: QWidget, p: TypePreset, metadata: Dict[str, Any]) -> None:
    while w.layout().count():
        removed_item = w.layout().takeAt(0)
        if removed_item is None:
            break
        if removed_item.widget() is not None:
            removed_item.widget().deleteLater()

    metadata.clear()

    form_layout = w.layout()
    if not isinstance(form_layout, QFormLayout):
        raise Exception("Type widget should have a form layout")
    if p == TypePreset.DOUBLE:

        metadata["range"] = None

        def set_range(new_range: NumericRange) -> None:
            metadata["range"] = new_range

        numeric_range_input = NumericRangeFormatWidget(numeric_range=None)
        numeric_range_input.range_changed.connect(set_range)
        form_layout.addRow("Value range", numeric_range_input)


class AttributiCrud(QWidget):
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

        with self._db.connect() as conn:
            self._attributi = [
                TabledAttributo(k, attributo)
                for k, attributi in self._db.retrieve_attributi(conn).items()
                for attributo in attributi.values()
            ]
            self._attributi_table = DeclarativeTable(data=self._create_table_data())

        root_widget.addWidget(self._attributi_table)

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

        self._attributo_id_edit = QLineEdit()
        right_form_layout.addRow(
            "ID",
            self._attributo_id_edit,
        )
        self._attributo_id_edit.textEdited.connect(self._attributo_id_change)

        self._attributo_description_edit = QLineEdit()
        right_form_layout.addRow(
            "Description",
            self._attributo_description_edit,
        )
        self._attributo_description_edit.textEdited.connect(
            self._attributi_description_change
        )

        self._attributo_suffix_edit = QLineEdit()
        right_form_layout.addRow(
            "Suffix",
            self._attributo_suffix_edit,
        )
        self._attributo_suffix_edit.textEdited.connect(self._attributo_suffix_change)

        self._attributi_table_combo = ComboBox(
            [(t.pretty_id(), t) for t in AssociatedTable], AssociatedTable.RUN
        )
        right_form_layout.addRow(
            "Table",
            self._attributi_table_combo,
        )
        self._attributi_table_combo.item_selected.connect(self._attributo_table_change)

        # Type begin
        self._type_selection = ComboBox(
            [(s.value, s) for s in TypePreset], TypePreset.INT
        )
        self._type_selection.item_selected.connect(self._refill_type_preset)
        self._type_widget = QWidget()
        QFormLayout(self._type_widget)
        self._type_specific_metadata: Dict[str, Any] = {}
        right_form_layout.addRow(
            "Type",
            self._type_selection,
        )
        right_form_layout.addWidget(self._type_widget)
        _fill_preset(
            self._type_widget,
            cast(TypePreset, self._type_selection.current_value()),
            self._type_specific_metadata,
        )
        # Type end

        self._submit_widget = QWidget()
        self._submit_layout = QHBoxLayout()
        self._submit_layout.setContentsMargins(0, 0, 0, 0)
        self._submit_widget.setLayout(self._submit_layout)
        self._add_button = self._create_add_button()
        self._add_button.setEnabled(False)
        self._submit_layout.addWidget(self._add_button)

        right_form_layout.addWidget(self._submit_widget)

    def _refill_type_preset(self, new_value: TypePreset) -> None:
        _fill_preset(
            self._type_widget,
            new_value,
            self._type_specific_metadata,
        )

    def _create_table_data(self) -> Data:
        return Data(
            rows=[
                Row(
                    display_roles=[
                        a.attributo.name,
                        a.attributo.description,
                        attributo_type_to_string(a.attributo),
                        a.table.pretty_id(),
                    ],
                    edit_roles=[None, None, None, None],
                    right_click_menu=partial(self._slot_right_click, a),
                )
                for a in self._attributi
            ],
            columns=[
                Column(header_label="ID", editable=False),
                Column(header_label="Description", editable=False),
                Column(header_label="Type", editable=False),
                Column(header_label="Table", editable=False),
            ],
            row_delegates={},
            column_delegates={},
        )

    def _slot_right_click(self, a: TabledAttributo, p: QPoint) -> None:
        menu = QMenu(self)
        deleteAction = menu.addAction(
            self.style().standardIcon(QStyle.SP_DialogCancelButton),
            "Delete attributo",
        )
        action = menu.exec_(p)
        if action == deleteAction:
            result = QMessageBox(  # type: ignore
                QMessageBox.Critical,
                f"Delete attributo “{a.attributo.name}”",
                f"Are you sure you want to delete attributo <b>“{a.attributo.pretty_id()}”</b> from table <b>“{a.table.pretty_id()}”</b>?",
                QMessageBox.Yes | QMessageBox.Cancel,
                self,
            ).exec()

            if result == QMessageBox.Yes:
                with self._db.connect() as conn:
                    self._db.delete_attributo(
                        conn,
                        a.table,
                        a.attributo.name,
                    )
                    self._slot_refresh(conn)

    def _slot_refresh(self, conn: Connection) -> None:
        self._attributi = [
            TabledAttributo(k, attributo)
            for k, attributi in self._db.retrieve_attributi(conn).items()
            for attributo in attributi.values()
        ]
        self._attributi_table.set_data(self._create_table_data())

    def _attributo_id_taken(
        self, table: AssociatedTable, attributo_id: AttributoId
    ) -> bool:
        return any(
            t
            for t in self._attributi
            if t.table == table and t.attributo.name == attributo_id
        )

    def _update_add_button(self) -> None:
        self._add_button.setEnabled(
            bool(self._attributo_id_edit.text())
            and not self._attributo_id_taken(
                cast(AssociatedTable, self._attributi_table_combo.current_value()),
                AttributoId(self._attributo_id_edit.text()),
            )
        )

    def _attributo_id_change(self, _new_text: str) -> None:
        self._update_add_button()

    def _attributo_suffix_change(self, _new_text: str) -> None:
        self._update_add_button()

    def _attributi_description_change(self, _new_text: str) -> None:
        self._update_add_button()

    def _attributo_table_change(self, _new_table: AssociatedTable) -> None:
        self._update_add_button()

    def _slot_new_attributo(self) -> None:
        new_column = new_attributo_dialog(self._attributi_table.metadata.keys(), self)

        if new_column is None:
            return

        with self._db.connect() as conn:
            self._db.add_attributo(
                conn,
                name=new_column.name,
                description=new_column.description,
                suffix=None,
                prop_type=new_column.rich_property_type,
                associated_table=AssociatedTable.RUN,
            )
            self._slot_refresh(conn)

    def _create_add_button(self):
        b = QPushButton(
            self.style().standardIcon(QStyle.SP_DialogOkButton), "Add sample"
        )
        b.clicked.connect(self._add_attributo)
        return b

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

    def _generate_rich_type(self) -> RichAttributoType:
        value = self._type_selection.current_value()
        if value == TypePreset.INT:
            return PropertyInt()
        if value == TypePreset.DOUBLE:
            return PropertyDouble(
                range=self._type_specific_metadata.get("range", None),
                suffix=self._attributo_suffix_edit.text(),
            )
        raise Exception(f"unsupported type {value}")

    def _add_attributo(self) -> None:
        with self._db.connect() as conn:
            self._db.add_attributo(
                conn,
                self._attributo_id_edit.text(),
                self._attributo_description_edit.text(),
                cast(AssociatedTable, self._attributi_table_combo.current_value()),
                self._attributo_suffix_edit.text(),
                self._generate_rich_type(),
            )
            self._reset_input_fields()
            self._slot_refresh(conn)
            self._log_widget.setText("Attributo added!")

    def _reset_input_fields(self) -> None:
        self._attributo_id_edit.setText("")
        self._attributo_description_edit.setText("")
        self._attributo_suffix_edit.setText("")
        self._type_selection.set_current_value(TypePreset.INT)
        self._refill_type_preset(TypePreset.INT)
        self._update_add_button()
