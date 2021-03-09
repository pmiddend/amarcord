import logging
from functools import partial
import datetime
from typing import Final, List

from PyQt5 import QtCore, QtWidgets
from PyQt5.QtCore import QPoint, QTimer, pyqtSignal
from PyQt5.QtWidgets import QCheckBox, QPushButton, QWidget

from amarcord.db.attributi import (
    AttributiMap,
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
from amarcord.qt.declarative_table import Column, Data, DeclarativeTable, Row
from amarcord.qt.infix_completer import InfixCompletingLineEdit
from amarcord.query_parser import Query, UnexpectedEOF, parse_query

AUTO_REFRESH_TIMER_MSEC: Final = 5000

logger = logging.getLogger(__name__)


class RunTable(QWidget):
    run_selected = pyqtSignal(int)

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
            self._visible_columns: List[AttributoId] = list(
                self._attributi_metadata.keys()
            )
            self._last_refresh = datetime.datetime.utcnow()
            self._runs: List[AttributiMap] = self._db.retrieve_runs(
                conn, self._proposal_id, None
            )
            self._filter_query: Query = lambda row: True
            self._table_view = DeclarativeTable(
                self._create_declarative_data(),
                parent=None,
            )

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

    def _create_declarative_data(self):
        return Data(
            rows=[
                Row(
                    display_roles=[
                        pretty_print_attributo(
                            self._attributi_metadata.get(r, None), c.select_value(r)
                        )
                        for r in self._visible_columns
                    ],
                    edit_roles=[],
                    background_roles={},
                    change_callbacks=[],
                    double_click_callback=lambda: self.run_selected.emit(
                        c.select_int_unsafe(self._db.tables.property_run_id)
                    ),
                )
                for c in self._runs
                if self._run_not_filtered(c)
            ],
            columns=[
                Column(
                    header_label=self._attributi_metadata[c].description
                    if self._attributi_metadata[c].description
                    else self._attributi_metadata[c].name,
                    editable=False,
                    header_callback=partial(self._header_menu_callback, c),
                )
                for c in self._visible_columns
            ],
            row_delegates={},
            column_delegates={},
        )

    def _run_not_filtered(self, run: AttributiMap) -> bool:
        return self._filter_query(run.to_query_row(self._attributi_metadata.keys()))

    def _slot_toggle_auto_refresh(self, _new_state: bool) -> None:
        if self._update_timer.isActive():
            self._update_timer.stop()
        else:
            self._update_timer.start(AUTO_REFRESH_TIMER_MSEC)

    def _slot_refresh(self) -> None:
        with self._db.connect() as conn:
            self._attributi_metadata = self._db.run_attributi(conn)
            completer = QtWidgets.QCompleter(
                list(self._attributi_metadata.keys()), self
            )
            completer.setCompletionMode(QtWidgets.QCompleter.InlineCompletion)
            self._filter_widget.setCompleter(completer)
            run_updates = self._db.retrieve_runs(
                conn, self._proposal_id, self._last_refresh
            )
            for idx in range(len(self._runs)):
                for update_idx, update in enumerate(run_updates):
                    if self._runs[idx].select_int_unsafe(
                        self._db.tables.property_run_id
                    ) == update.select_int_unsafe(self._db.tables.property_run_id):
                        logger.info(
                            "Updating run %s",
                            self._runs[idx].select_int_unsafe(
                                self._db.tables.property_run_id
                            ),
                        )
                        self._runs[idx] = update
                        run_updates.pop(update_idx)
                        break
                    if not run_updates:
                        break
            for new_run in run_updates:
                logger.info(
                    "Appending new run %s",
                    new_run.select_int_unsafe(self._db.tables.property_run_id),
                )
                self._runs.append(new_run)
            self._last_refresh = datetime.datetime.utcnow()
            self._table_view.set_data(self._create_declarative_data())

    def _header_menu_callback(self, column: AttributoId, pos: QPoint) -> None:
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

    def run_changed(self) -> None:
        self._slot_refresh()

    def _slot_switch_columns(self) -> None:
        new_columns = display_column_chooser(
            self, self._visible_columns, self._attributi_metadata
        )
        self._visible_columns = new_columns
        self._slot_refresh()

    def _late_init(self) -> None:
        self._slot_refresh()

    def _slot_filter_changed(self, f: str) -> None:
        try:
            self._filter_query = parse_query(f, set(self._attributi_metadata.keys()))
            self._slot_refresh()
        except UnexpectedEOF:
            self._query_error.setText("")
        except Exception as e:
            self._query_error.setText(f"{e}")
