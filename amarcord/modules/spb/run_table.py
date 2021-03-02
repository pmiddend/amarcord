import logging
import datetime
from typing import Any, Dict, Final, List, Optional, Set

import pandas as pd
from PyQt5 import QtCore, QtWidgets
from PyQt5.QtCore import QTimer
from PyQt5.QtWidgets import QCheckBox, QPushButton
from matplotlib.backends.backend_qt5agg import (
    FigureCanvasQTAgg,
    NavigationToolbar2QT as NavigationToolbar,
)
from matplotlib.figure import Figure

from amarcord.modules.context import Context
from amarcord.modules.spb.column_chooser import display_column_chooser
from amarcord.modules.spb.filter_query_help import filter_query_help
from amarcord.modules.spb.proposal_id import ProposalId
from amarcord.modules.spb.db import DBRunComment, DB
from amarcord.modules.spb.run_property import RunProperty
from amarcord.modules.spb.db_tables import DBTables
from amarcord.qt.infix_completer import InfixCompletingLineEdit
from amarcord.qt.properties import PropertyDouble, PropertyInt
from amarcord.qt.table import GeneralTableWidget
from amarcord.query_parser import UnexpectedEOF, parse_query

AUTO_REFRESH_TIMER_MSEC: Final = 5000

logger = logging.getLogger(__name__)


Row = Dict[RunProperty, Any]


def _convert_comment_column(comments: List[DBRunComment], role: int) -> Any:
    if role == QtCore.Qt.DisplayRole:
        return "\n".join(f"{c.author}: {c.text}" for c in comments)
    if role == QtCore.Qt.EditRole:
        return comments
    return QtCore.QVariant()


class _MplCanvas(FigureCanvasQTAgg):
    def __init__(self, width=5, height=4, dpi=100):
        fig = Figure(figsize=(width, height), dpi=dpi)
        self.axes = fig.add_subplot(111)
        super().__init__(fig)


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
            self._run_property_names = self._db.run_property_metadata(conn)
            self._table_view = GeneralTableWidget[RunProperty](
                enum_type_retriever=lambda: list(self._run_property_names.keys()),
                column_header_retriever=lambda: {
                    k: v.description if v.description else v.name
                    for k, v in self._run_property_names.items()
                },
                column_visibility=list(self._run_property_names.keys()),
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
        filter_widget = InfixCompletingLineEdit(self)
        filter_widget.textChanged.connect(self._slot_filter_changed)
        completer = QtWidgets.QCompleter(list(self._run_property_names.keys()), self)
        completer.setCompletionMode(QtWidgets.QCompleter.InlineCompletion)
        filter_widget.setCompleter(completer)
        inner_filter_layout.addWidget(filter_widget)
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
            self._run_property_names = self._db.run_property_metadata(conn)
            self._table_view.refresh()

    def _convert_column(self, run_property: RunProperty, role: int, value: Any) -> str:
        if run_property == self._db.tables.property_comments:
            assert isinstance(value, list), "Comment column isn't a list"
            return _convert_comment_column(value, role)
        if isinstance(value, list):
            return ", ".join(value)
        if isinstance(value, float):
            return f"{value:.2f}"
        return str(value) if value is not None else ""

    def _header_menu_callback(self, pos: QtCore.QPoint, column: RunProperty) -> None:
        property_metadata = self._run_property_names.get(column, None)
        if property_metadata is None or property_metadata.rich_prop_type is None:
            return
        if not isinstance(
            property_metadata.rich_prop_type, (PropertyInt, PropertyDouble)
        ):
            return
        started_property = RunProperty("started")
        if started_property not in self._run_property_names:
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

            sc = _MplCanvas(width=5, height=4, dpi=100)

            df = pd.DataFrame(
                self._table_view.get_filtered_column_values(column),
                index=[
                    datetime.datetime.fromisoformat(s)
                    for s in self._table_view.get_filtered_column_values(
                        started_property
                    )
                ],
                columns=["Values"],
            )

            # noinspection PyArgumentList
            df.plot(ax=sc.axes)  # type: ignore

            toolbar = NavigationToolbar(sc, self)

            dialog_layout.addWidget(toolbar)
            dialog_layout.addWidget(sc)
            button_box = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Close)
            button_box.rejected.connect(dialog.reject)
            dialog_layout.addWidget(button_box)

            dialog.exec()

    def _slot_row_selected(self, row: Dict[RunProperty, Any]) -> None:
        self.run_selected.emit(row[self._db.tables.property_run_id])

    def run_changed(self) -> None:
        self._slot_refresh()

    def _slot_switch_columns(self) -> None:
        new_columns = display_column_chooser(
            self, self._table_view.column_visibility, self._run_property_names
        )
        self._table_view.set_column_visibility(new_columns)

    def _retrieve_runs(self, since: Optional[datetime.datetime]) -> List[Row]:
        with self._db.connect() as conn:
            return self._db.retrieve_runs(conn, self._proposal_id, since)

    def _late_init(self) -> None:
        self._table_view.set_data_retriever(self._retrieve_runs)

    def _slot_filter_changed(self, f: str) -> None:
        try:
            query = parse_query(f, set(self._run_property_names.keys()))
            self._table_view.set_filter_query(query)
        except UnexpectedEOF:
            self._query_error.setText("")
        except Exception as e:
            self._query_error.setText(f"{e}")
