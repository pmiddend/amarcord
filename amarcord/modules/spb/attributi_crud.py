import logging
from enum import Enum
from functools import partial
from typing import Any
from typing import Callable
from typing import Dict
from typing import Final
from typing import Optional
from typing import cast

from PyQt5.QtCore import QPoint
from PyQt5.QtCore import QTimer
from PyQt5.QtCore import QVariant
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QCheckBox
from PyQt5.QtWidgets import QFormLayout
from PyQt5.QtWidgets import QHBoxLayout
from PyQt5.QtWidgets import QLabel
from PyQt5.QtWidgets import QLineEdit
from PyQt5.QtWidgets import QMenu
from PyQt5.QtWidgets import QMessageBox
from PyQt5.QtWidgets import QPushButton
from PyQt5.QtWidgets import QSplitter
from PyQt5.QtWidgets import QStyle
from PyQt5.QtWidgets import QVBoxLayout
from PyQt5.QtWidgets import QWidget
from pint import DefinitionSyntaxError
from pint import UndefinedUnitError
from pint import UnitRegistry

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributi import (
    attributo_type_to_string,
)
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoType
from amarcord.db.attributo_type import AttributoTypeDouble
from amarcord.db.attributo_type import AttributoTypeInt
from amarcord.db.attributo_type import AttributoTypeString
from amarcord.db.attributo_type import AttributoTypeTags
from amarcord.db.attributo_type import AttributoTypeUserName
from amarcord.db.db import Connection
from amarcord.db.db import DB
from amarcord.db.proposal_id import ProposalId
from amarcord.db.tabled_attributo import TabledAttributo
from amarcord.db.tables import DBTables
from amarcord.modules.context import Context
from amarcord.modules.password_check_dialog import password_check_dialog
from amarcord.numeric_range import NumericRange
from amarcord.qt.combo_box import ComboBox
from amarcord.qt.declarative_table import Column
from amarcord.qt.declarative_table import Data
from amarcord.qt.declarative_table import DeclarativeTable
from amarcord.qt.declarative_table import Row
from amarcord.qt.numeric_range_format_widget import NumericRangeFormatWidget
from amarcord.qt.validated_line_edit import ValidatedInputValue
from amarcord.qt.validated_line_edit import ValidatedLineEdit
from amarcord.qt.validators import Partial

DATE_TIME_FORMAT = "%Y-%m-%d %H:%M"

NEW_SAMPLE_HEADLINE: Final = "New attributo"
AUTO_REFRESH_TIMER_MSEC: Final = 5000

logger = logging.getLogger(__name__)


class TypePreset(Enum):
    INT = "integer"
    DOUBLE = "number"
    PERCENT = "percent"
    STANDARD_UNIT = "standard unit"
    TAGS = "list of tags"
    STRING = "string"
    USER_NAME = "user name"


def _unit_to_string(u: str) -> str:
    return u


def _string_to_unit(ureg: UnitRegistry, s: str) -> ValidatedInputValue:
    try:
        ureg(s)
        return QVariant(s)
    except UndefinedUnitError:
        return Partial(s)
    except DefinitionSyntaxError:
        return Partial(s)


def _fill_preset(
    w: QWidget,
    ureg: UnitRegistry,
    p: TypePreset,
    metadata: Dict[str, Any],
    metadata_change: Callable[[], None],
) -> None:
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
        metadata["suffix"] = None

        def set_range(new_range: NumericRange) -> None:
            metadata["range"] = new_range

        def set_suffix(s: str) -> None:
            metadata["suffix"] = s

        numeric_range_input = NumericRangeFormatWidget(numeric_range=None)
        numeric_range_input.range_changed.connect(set_range)
        form_layout.addRow("Value range", numeric_range_input)

        attributo_suffix_edit = QLineEdit()
        form_layout.addRow("Suffix", attributo_suffix_edit)
        attributo_suffix_edit.textEdited.connect(set_suffix)
    elif p == TypePreset.STANDARD_UNIT:
        metadata["range"] = None
        metadata["suffix"] = None
        metadata["valid"] = False

        def set_range(new_range: NumericRange) -> None:
            metadata["range"] = new_range

        def set_standard_suffix(l: QLabel, s: ValidatedInputValue) -> None:
            metadata["suffix"] = s
            if not isinstance(s, Partial):
                # noinspection PyStringFormat
                l.setText("Normalized: {:H}".format(ureg(s)))  # type: ignore
                metadata["valid"] = True
            else:
                metadata["valid"] = False
            metadata_change()

        numeric_range_input = NumericRangeFormatWidget(numeric_range=None)
        numeric_range_input.range_changed.connect(set_range)
        form_layout.addRow("Value range", numeric_range_input)

        attributo_suffix_layout = QVBoxLayout()
        attributo_suffix_edit = ValidatedLineEdit(
            None, _unit_to_string, partial(_string_to_unit, ureg), "standard unit (ml, GJ, ...)"  # type: ignore
        )
        attributo_suffix_layout.addWidget(attributo_suffix_edit)
        normalized_label = QLabel("Normalized: ")
        normalized_label.setTextFormat(Qt.RichText)
        normalized_label.setStyleSheet("QLabel { font: italic 10px; color: grey; }")
        attributo_suffix_layout.addWidget(normalized_label)
        form_layout.addRow("Suffix", attributo_suffix_layout)
        attributo_suffix_edit.value_change.connect(
            partial(set_standard_suffix, normalized_label)
        )


class AttributiCrud(QWidget):
    def __init__(
        self,
        context: Context,
        tables: DBTables,
        proposal_id: ProposalId,
        parent: Optional[QWidget] = None,
    ) -> None:
        super().__init__(parent)

        self._proposal_id = proposal_id
        self._db = DB(context.db, tables)

        root_layout = QHBoxLayout()
        self.setLayout(root_layout)
        root_layout.setContentsMargins(0, 0, 0, 0)

        root_widget = QSplitter(self)
        root_layout.addWidget(root_widget)

        with self._db.connect() as conn:
            self._attributi = [
                TabledAttributo(k, attributo)
                for k, attributi in self._db.retrieve_attributi(conn).items()
                for attributo in attributi.values()
            ]
            self._attributi_table = DeclarativeTable(data=self._create_table_data())

        left_column = QWidget()
        left_column_layout = QVBoxLayout(left_column)
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
                "<b>Right-click</b> column header to show options. Editing not possible yet."
            )
        )
        left_column_layout.addWidget(self._attributi_table)
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
        self._add_button = self._create_add_button()
        _fill_preset(
            self._type_widget,
            UnitRegistry(),
            cast(TypePreset, self._type_selection.current_value()),
            self._type_specific_metadata,
            self._update_add_button,
        )
        # Type end

        self._submit_widget = QWidget()
        self._submit_layout = QHBoxLayout()
        self._submit_layout.setContentsMargins(0, 0, 0, 0)
        self._submit_widget.setLayout(self._submit_layout)
        self._add_button.setEnabled(False)
        self._submit_layout.addWidget(self._add_button)

        right_form_layout.addWidget(self._submit_widget)

        self._update_timer = QTimer(self)
        self._update_timer.timeout.connect(self._slot_refresh_with_conn)

    def _slot_toggle_auto_refresh(self, _new_state: bool) -> None:
        if self._update_timer.isActive():
            self._update_timer.stop()
        else:
            self._update_timer.start(AUTO_REFRESH_TIMER_MSEC)

    def hideEvent(self, _e: Any) -> None:
        self._update_timer.stop()

    def showEvent(self, _e: Any) -> None:
        if self._auto_refresh.isChecked():
            self._slot_refresh_with_conn()
            self._update_timer.start(AUTO_REFRESH_TIMER_MSEC)

    def regenerate_for_table(self, t: AssociatedTable) -> None:
        self._reset_input_fields()
        self._attributi_table_combo.set_current_value(t)

    def _refill_type_preset(self, new_value: TypePreset) -> None:
        _fill_preset(
            self._type_widget,
            UnitRegistry(),
            new_value,
            self._type_specific_metadata,
            self._update_add_button,
        )
        self._update_add_button()

    def _create_table_data(self) -> Data:
        return Data(
            rows=[
                Row(
                    display_roles=[
                        a.attributo.name,
                        a.attributo.description,
                        attributo_type_to_string(a.attributo.attributo_type),
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
            password = password_check_dialog(
                f"Delete attributo “{a.attributo.name}”",
                f"Are you sure you want to delete attributo <b>“{a.attributo.pretty_id()}”</b> from table <b>“{a.table.pretty_id()}”</b>?",
            )

            if password:
                with self._db.connect() as conn:
                    if not self._db.check_proposal_password(
                        conn, self._proposal_id, password
                    ):
                        self._show_invalid_password()
                    else:
                        self._db.delete_attributo(
                            conn,
                            a.table,
                            a.attributo.name,
                        )
                        self._slot_refresh(conn)

    def _show_invalid_password(self):
        QMessageBox.critical(  # type: ignore
            self,
            "Invalid password",
            "Password was invalid!",
            QMessageBox.Ok,
        )

    def _slot_refresh_with_conn(self) -> None:
        with self._db.connect() as conn:
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
            and self._type_specific_metadata.get("valid", True)
        )

    def _attributo_id_change(self, _new_text: str) -> None:
        self._update_add_button()

    def _attributi_description_change(self, _new_text: str) -> None:
        self._update_add_button()

    def _attributo_table_change(self, _new_table: AssociatedTable) -> None:
        self._update_add_button()

    def _create_add_button(self):
        b = QPushButton(
            self.style().standardIcon(QStyle.SP_DialogOkButton), "Add attributo"
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

    def _generate_rich_type(self) -> AttributoType:
        value = self._type_selection.current_value()
        if value == TypePreset.INT:
            return AttributoTypeInt()
        if value == TypePreset.PERCENT:
            return AttributoTypeDouble(
                range=NumericRange(0, True, 100, True),
                suffix="%",
            )
        if value == TypePreset.STANDARD_UNIT:
            return AttributoTypeDouble(
                range=self._type_specific_metadata.get("range", None),
                suffix=self._type_specific_metadata.get("suffix", None),
            )
        if value == TypePreset.DOUBLE:
            return AttributoTypeDouble(
                range=self._type_specific_metadata.get("range", None),
                suffix=self._type_specific_metadata.get("suffix", None),
            )
        if value == TypePreset.STRING:
            return AttributoTypeString()
        if value == TypePreset.USER_NAME:
            return AttributoTypeUserName()
        if value == TypePreset.TAGS:
            return AttributoTypeTags()
        raise Exception(f"unsupported type {value}")

    def _add_attributo(self) -> None:
        with self._db.connect() as conn:
            password = password_check_dialog(
                "Add attributo",
                "Please enter the admin password to add the attributo.",
            )

            if not password:
                return

            if not self._db.check_proposal_password(conn, self._proposal_id, password):
                self._show_invalid_password()
                return

            self._db.add_attributo(
                conn,
                self._attributo_id_edit.text(),
                self._attributo_description_edit.text(),
                cast(AssociatedTable, self._attributi_table_combo.current_value()),
                self._generate_rich_type(),
            )
            self._reset_input_fields()
            self._slot_refresh(conn)
            self._log_widget.setText("Attributo added!")

    def _reset_input_fields(self) -> None:
        self._attributo_id_edit.setText("")
        self._attributo_description_edit.setText("")
        self._type_selection.set_current_value(TypePreset.INT)
        self._refill_type_preset(TypePreset.INT)
        self._update_add_button()
