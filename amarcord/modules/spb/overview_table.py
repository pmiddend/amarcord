import datetime
import logging
from functools import partial
from typing import Any, Final, Iterable, List, Tuple

from PyQt5 import QtCore, QtWidgets
from PyQt5.QtCore import QPoint, QTimer, Qt, pyqtSignal
from PyQt5.QtWidgets import QCheckBox, QMenu, QMessageBox, QPushButton, QStyle, QWidget

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributi import (
    PropertyDouble,
    PropertyInt,
    pretty_print_attributo,
    sortable_attributo,
)
from amarcord.db.db import Connection, DB, OverviewAttributi, overview_row_to_query_row
from amarcord.db.proposal_id import ProposalId
from amarcord.db.tabled_attributo import TabledAttributo
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


class OverviewTable(QWidget):
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
            self._attributi_metadata: List[
                TabledAttributo
            ] = self._retrieve_attributi_metadata(conn)
            self._visible_columns = self._attributi_metadata.copy()
            self._last_refresh = datetime.datetime.utcnow()
            self._last_run_count = self._db.run_count(conn)
            self._rows: List[OverviewAttributi] = self._db.retrieve_overview(
                conn, self._proposal_id
            )
            self._filter_query: Query = lambda row: True
            self._sort_data: Tuple[TabledAttributo, Qt.SortOrder] = (
                TabledAttributo(
                    AssociatedTable.RUN,
                    self._db.tables.additional_attributi[AssociatedTable.RUN][
                        self._db.tables.attributo_run_id
                    ],
                ),
                Qt.AscendingOrder,
            )
            self._table_view = DeclarativeTable(
                self._create_declarative_data(),
                parent=None,
            )
            self._table_view.setSortingEnabled(True)

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
        completer = QtWidgets.QCompleter(self._filter_query_completions(), self)
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

    def hideEvent(self, e) -> None:
        self._update_timer.stop()

    def showEvent(self, e) -> None:
        self._update_timer.start(AUTO_REFRESH_TIMER_MSEC)

    def _retrieve_attributi_metadata(self, conn: Connection) -> List[TabledAttributo]:
        return [
            TabledAttributo(k, attributo)
            for k, attributi in self._db.retrieve_attributi(conn).items()
            for attributo in attributi.values()
        ]

    def _filter_query_completions(self) -> Iterable[str]:
        return [s.technical_id() for s in self._attributi_metadata]

    def _create_declarative_data(self):
        return Data(
            rows=[
                Row(
                    display_roles=[
                        pretty_print_attributo(
                            r.attributo, c[r.table].select_value(r.attributo.name)
                        )
                        for r in self._visible_columns
                    ],
                    edit_roles=[
                        sortable_attributo(
                            r.attributo, c[r.table].select_value(r.attributo.name)
                        )
                        for r in self._visible_columns
                    ],
                    background_roles={},
                    change_callbacks=[],
                    double_click_callback=partial(self._double_click, c),
                    right_click_menu=partial(self._right_click, c),
                )
                for c in self._rows
                if self._row_not_filtered(c)
            ],
            columns=[
                Column(
                    header_label=c.pretty_id(),
                    editable=False,
                    header_callback=partial(self._header_menu_callback, c),
                    sort_click_callback=partial(self._sort_clicked, c),
                    sorted_by=self._sort_data[1] if self._sort_data[0] == c else None,
                )
                for c in self._visible_columns
            ],
            row_delegates={},
            column_delegates={},
        )

    def _right_click(self, c: OverviewAttributi, p: QPoint) -> None:
        menu = QMenu(self)
        deleteAction = menu.addAction(
            self.style().standardIcon(QStyle.SP_DialogCancelButton),
            "Delete run",
        )
        run_id = c[AssociatedTable.RUN].select_int_unsafe(
            self._db.tables.attributo_run_id
        )
        action = menu.exec_(p)
        if action == deleteAction:
            result = QMessageBox(  # type: ignore
                QMessageBox.Critical,
                f"Delete run “{run_id}”",
                f"Are you sure you want to delete run “{run_id}”?",
                QMessageBox.Yes | QMessageBox.Cancel,
                self,
            ).exec()

            if result == QMessageBox.Yes:
                with self._db.connect() as conn:
                    self._db.delete_run(
                        conn,
                        run_id,
                    )
                    self._slot_refresh()

    def _double_click(self, c: OverviewAttributi) -> None:
        self.run_selected.emit(
            c[AssociatedTable.RUN].select_int_unsafe(self._db.tables.attributo_run_id)
        )

    def _row_not_filtered(self, row: OverviewAttributi) -> bool:
        return self._filter_query(
            overview_row_to_query_row(row, self._attributi_metadata)
        )

    def _slot_toggle_auto_refresh(self, _new_state: bool) -> None:
        if self._update_timer.isActive():
            self._update_timer.stop()
        else:
            self._update_timer.start(AUTO_REFRESH_TIMER_MSEC)

    def _slot_refresh(self, force: bool = False) -> None:
        with self._db.connect() as conn:
            self._attributi_metadata = self._retrieve_attributi_metadata(conn)
            completer = QtWidgets.QCompleter(self._filter_query_completions(), self)
            completer.setCompletionMode(QtWidgets.QCompleter.InlineCompletion)
            self._filter_widget.setCompleter(completer)
            latest_update_time = self._db.overview_update_time(conn)
            latest_run_count = self._db.run_count(conn)
            needs_update = (
                latest_update_time > self._last_refresh
                or latest_run_count != self._last_run_count
            )
            if needs_update:
                self._rows = self._db.retrieve_overview(conn, self._proposal_id)
                self._last_refresh = latest_update_time
                self._last_run_count = latest_run_count
                self._rows.sort(
                    key=self._sort_key,
                    reverse=(self._sort_data[1] == Qt.AscendingOrder),
                )
            if needs_update or force:
                self._table_view.set_data(self._create_declarative_data())

    def _sort_key(self, k: OverviewAttributi) -> Any:
        sort_column = self._sort_data[0]
        table_data = k[sort_column.table]
        if table_data is None:
            return None
        v = table_data.select_value(sort_column.attributo.name)
        return sortable_attributo(sort_column.attributo, v)

    def _sort_clicked(self, column: TabledAttributo, order: Qt.SortOrder) -> None:
        self._sort_data = (column, order)
        self._rows.sort(
            key=self._sort_key, reverse=(self._sort_data[1] == Qt.AscendingOrder)
        )
        self._table_view.set_data(self._create_declarative_data())

    def _header_menu_callback(self, column: TabledAttributo, pos: QPoint) -> None:
        if column.attributo.rich_property_type is None:
            return
        if not isinstance(
            column.attributo.rich_property_type, (PropertyInt, PropertyDouble)
        ):
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
                self._db, self._proposal_id, self._filter_query, column, self
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
            self._filter_query = parse_query(
                f, set(k.technical_id() for k in self._attributi_metadata)
            )
            logger.info("Filter query changed successfully, refreshing")
            self._slot_refresh(force=True)
        except UnexpectedEOF:
            self._query_error.setText("")
        except Exception as e:
            self._query_error.setText(f"{e}")
