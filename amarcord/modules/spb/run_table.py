import datetime
import logging
from typing import Any, Dict, Final, List, Optional

from PyQt5 import QtCore, QtWidgets
from PyQt5.QtCore import QTimer
from PyQt5.QtWidgets import QCheckBox, QPushButton

from amarcord.db.attributi import (
    PropertyDouble,
    PropertyInt,
    pretty_print_attributo,
)
from amarcord.db.attributo_id import AttributoId
from amarcord.db.db import DB
from amarcord.db.proposal_id import ProposalId
from amarcord.db.tables import DBTables
from amarcord.modules.context import Context
from amarcord.modules.spb.column_chooser import display_column_chooser
from amarcord.modules.spb.filter_query_help import filter_query_help
from amarcord.modules.spb.plot_dialog import PlotDialog
from amarcord.qt.infix_completer import InfixCompletingLineEdit
from amarcord.qt.table import GeneralTableWidget
from amarcord.query_parser import UnexpectedEOF, parse_query

AUTO_REFRESH_TIMER_MSEC: Final = 5000

logger = logging.getLogger(__name__)


Row = Dict[AttributoId, Any]


class RunTable(QtWidgets.QWidget):
    run_selected = QtCore.pyqtSignal(int)

    def __init__(
        self, context: Context, tables: DBTables, proposal_id: ProposalId
    ) -> None:
        super().__init__()

        self._proposal_id = proposal_id
        self._db = DB(context.db, tables)

        # Init main widgets
        refresh_button = QPushButton(
            self.style().standardIcon(QtWidgets.QStyle.SP_BrowserReload),
            "Refresh",
        )
        refresh_button.clicked.connect(self._slot_refresh)

        choose_columns = QPushButton(
            self.style().standardIcon(QtWidgets.QStyle.SP_FileDialogDetailedView),
            "Choose columns",
        )
        choose_columns.clicked.connect(self._slot_switch_columns)

        self._context = context
        with self._db.connect() as conn:
            self._attributi_metadata = self._db.run_attributi(conn)
            self._table_view = GeneralTableWidget[AttributoId](
                enum_type_retriever=lambda: list(self._attributi_metadata.keys()),
                column_header_retriever=lambda: {
                    k: v.description if v.description else v.name
                    for k, v in self._attributi_metadata.items()
                },
                column_visibility=list(self._attributi_metadata.keys()),
                column_converter=self._convert_column,
                primary_key_getter=lambda row: row[self._db.tables.property_run_id],
                parent=self,
            )
            self._table_view.set_menu_callback(self._header_menu_callback)
            self._table_view.row_double_click.connect(self._slot_row_selected)

        log_output = QtWidgets.QPlainTextEdit()
        log_output.setReadOnly(True)

        root_layout = QtWidgets.QVBoxLayout(self)

        header_layout = QtWidgets.QHBoxLayout()
        auto_refresh = QCheckBox("Auto refresh")
        auto_refresh.setChecked(True)
        auto_refresh.toggled.connect(self._slot_toggle_auto_refresh)
        header_layout.addWidget(auto_refresh, 0, QtCore.Qt.AlignTop)
        header_layout.addWidget(refresh_button, 0, QtCore.Qt.AlignTop)
        header_layout.addWidget(choose_columns, 0, QtCore.Qt.AlignTop)

        filter_query_layout = QtWidgets.QFormLayout()
        header_layout.addLayout(filter_query_layout)

        inner_filter_widget = QtWidgets.QWidget()
        inner_filter_layout = QtWidgets.QHBoxLayout()
        inner_filter_layout.setContentsMargins(0, 0, 0, 0)
        inner_filter_widget.setLayout(inner_filter_layout)
        self._filter_widget = InfixCompletingLineEdit(parent=self)
        self._filter_widget.textChanged.connect(self._slot_filter_changed)
        completer = QtWidgets.QCompleter(list(self._attributi_metadata.keys()), self)
        completer.setCompletionMode(QtWidgets.QCompleter.InlineCompletion)
        self._filter_widget.setCompleter(completer)
        inner_filter_layout.addWidget(self._filter_widget)
        icon_button = QPushButton(
            self.style().standardIcon(QtWidgets.QStyle.SP_MessageBoxQuestion), ""
        )
        icon_button.setStyleSheet("QPushButton{border:none;}")
        icon_button.clicked.connect(lambda: filter_query_help(self))
        inner_filter_layout.addWidget(icon_button)
        filter_query_layout.addRow("Filter query:", inner_filter_widget)

        self._query_error = QtWidgets.QLabel("")
        self._query_error.setStyleSheet("QLabel { font: italic 10px; color: red; }")

        filter_query_layout.addRow("", self._query_error)

        root_layout.addLayout(header_layout)
        root_layout.addWidget(
            QtWidgets.QLabel(
                "<b>Double-click</b> row to view details. <b>Right-click</b> column header to show options."
            )
        )

        root_layout.addWidget(self._table_view)
        context.db.after_db_created(self._late_init)

        self._update_timer = QTimer(self)
        self._update_timer.timeout.connect(self._slot_refresh)
        self._update_timer.start(AUTO_REFRESH_TIMER_MSEC)

    def _slot_toggle_auto_refresh(self, _new_state: bool) -> None:
        if self._update_timer.isActive():
            self._update_timer.stop()
        else:
            self._update_timer.start(AUTO_REFRESH_TIMER_MSEC)

    def _slot_refresh(self) -> None:
        with self._db.connect() as conn:
            self._attributi_metadata = self._db.run_attributi(conn)
            self._table_view.refresh()

    def _convert_column(self, run_property: AttributoId, _role: int, value: Any) -> str:
        return pretty_print_attributo(
            self._attributi_metadata.get(run_property, None)
            if self._attributi_metadata is not None
            else None,
            value,
        )

    def _header_menu_callback(self, pos: QtCore.QPoint, column: AttributoId) -> None:
        property_metadata = self._attributi_metadata.get(column, None)
        if property_metadata is None or property_metadata.rich_property_type is None:
            return
        if not isinstance(
            property_metadata.rich_property_type, (PropertyInt, PropertyDouble)
        ):
            return
        started_property = AttributoId("started")
        if started_property not in self._attributi_metadata:
            return
        menu = QtWidgets.QMenu(self)
        plotAction = menu.addAction(
            self.style().standardIcon(QtWidgets.QStyle.SP_DesktopIcon),
            "Plot this column",
        )
        action = menu.exec_(pos)
        if action == plotAction:
            dialog = QtWidgets.QDialog(self)
            dialog_layout = QtWidgets.QVBoxLayout()
            dialog.setLayout(dialog_layout)

            plot_dialog = PlotDialog(
                self._db, self._proposal_id, self._filter_widget.text(), column, self
            )
            plot_dialog.show()

    def _slot_row_selected(self, row: Dict[AttributoId, Any]) -> None:
        self.run_selected.emit(row[self._db.tables.property_run_id])

    def run_changed(self) -> None:
        self._slot_refresh()

    def _slot_switch_columns(self) -> None:
        new_columns = display_column_chooser(
            self, self._table_view.column_visibility, self._attributi_metadata
        )
        self._table_view.set_column_visibility(new_columns)

    def _retrieve_runs(self, since: Optional[datetime.datetime]) -> List[Row]:
        with self._db.connect() as conn:
            return self._db.retrieve_runs(conn, self._proposal_id, since)

    def _late_init(self) -> None:
        self._table_view.set_data_retriever(self._retrieve_runs)

    def _slot_filter_changed(self, f: str) -> None:
        try:
            query = parse_query(f, set(self._attributi_metadata.keys()))
            self._table_view.set_filter_query(query)
        except UnexpectedEOF:
            self._query_error.setText("")
        except Exception as e:
            self._query_error.setText(f"{e}")
