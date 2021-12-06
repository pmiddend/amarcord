import datetime
import logging
from functools import partial
from typing import Any
from typing import Dict
from typing import Final
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple

from PyQt5 import QtCore
from PyQt5 import QtWidgets
from PyQt5.QtCore import QPoint
from PyQt5.QtCore import QStringListModel
from PyQt5.QtCore import QTimer
from PyQt5.QtCore import pyqtSignal
from PyQt5.QtGui import QBrush
from PyQt5.QtGui import QColor
from PyQt5.QtWidgets import QCheckBox
from PyQt5.QtWidgets import QMenu
from PyQt5.QtWidgets import QMessageBox
from PyQt5.QtWidgets import QPushButton
from PyQt5.QtWidgets import QStyle
from PyQt5.QtWidgets import QWidget

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributi import pretty_print_attributo
from amarcord.db.attributi import sortable_attributo
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoType
from amarcord.db.attributo_type import AttributoTypeDouble
from amarcord.db.attributo_type import AttributoTypeDuration
from amarcord.db.attributo_type import AttributoTypeInt
from amarcord.db.db import Connection
from amarcord.db.db import DB
from amarcord.db.db import OverviewAttributi
from amarcord.db.db import overview_row_to_query_row
from amarcord.db.proposal_id import ProposalId
from amarcord.db.tabled_attributo import TabledAttributo
from amarcord.db.tables import DBTables
from amarcord.modules.attributi_change_dialog import apply_run_changes
from amarcord.modules.attributi_change_dialog import attributi_change_dialog
from amarcord.modules.context import Context
from amarcord.modules.password_check_dialog import password_check_dialog
from amarcord.modules.spb.colors import PREDEFINED_COLORS_TO_HEX
from amarcord.modules.spb.colors import name_to_hex
from amarcord.modules.spb.column_chooser import display_column_chooser
from amarcord.modules.spb.filter_query_help import filter_query_help
from amarcord.modules.spb.plot_dialog import PlotDialog
from amarcord.modules.spb.plot_dialog import _PlotType
from amarcord.qt.declarative_table import Column
from amarcord.qt.declarative_table import Data
from amarcord.qt.declarative_table import DeclarativeTable
from amarcord.qt.declarative_table import Row
from amarcord.qt.declarative_table import SortOrder
from amarcord.qt.infix_completer import InfixCompletingLineEdit
from amarcord.query_parser import Query
from amarcord.query_parser import UnexpectedEOF
from amarcord.query_parser import parse_query

AUTO_REFRESH_TIMER_MSEC: Final = 5000

logger = logging.getLogger(__name__)


def _plottable_attributo_type(t: AttributoType) -> bool:
    return isinstance(t, (AttributoTypeDouble, AttributoTypeInt, AttributoTypeDuration))


def _attributo_sort_key(t: TabledAttributo) -> Tuple[int, bool, str]:
    return t.table.sort_key(), t.attributo.name != "id", t.attributo.name


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
            self._attributi_metadata.sort(key=_attributo_sort_key)
            self._visible_columns = self._attributi_metadata.copy()
            self._samples = self._db.retrieve_mini_samples(conn, self._proposal_id)
            self._last_refresh: Optional[datetime.datetime] = datetime.datetime.utcnow()
            self._last_run_count = self._db.run_count(conn)
            self._rows: List[OverviewAttributi] = self._db.retrieve_overview(
                conn, self._proposal_id, self._db.retrieve_attributi(conn)
            )
            self._filter_query: Query = lambda row: True
            self._sort_data: Tuple[TabledAttributo, SortOrder] = (
                TabledAttributo(
                    AssociatedTable.RUN,
                    self._db.tables.additional_attributi[AssociatedTable.RUN][
                        self._db.tables.attributo_run_id
                    ],
                ),
                SortOrder.ASC,
            )
            self._table_view = DeclarativeTable(
                self._create_declarative_data(),
                parent=None,
            )

        log_output = QtWidgets.QPlainTextEdit()
        log_output.setReadOnly(True)

        root_layout = QtWidgets.QVBoxLayout(self)

        header_layout = QtWidgets.QHBoxLayout()
        self._auto_refresh = QCheckBox("Auto refresh")
        self._auto_refresh.setChecked(True)
        self._auto_refresh.toggled.connect(self._slot_toggle_auto_refresh)
        header_layout.addWidget(refresh_button, 0, QtCore.Qt.AlignTop)
        header_layout.addWidget(self._auto_refresh, 0, QtCore.Qt.AlignTop)
        header_layout.addWidget(choose_columns, 0, QtCore.Qt.AlignTop)

        filter_query_layout = QtWidgets.QFormLayout()
        header_layout.addLayout(filter_query_layout)

        inner_filter_widget = QtWidgets.QWidget()
        inner_filter_layout = QtWidgets.QHBoxLayout()
        inner_filter_layout.setContentsMargins(0, 0, 0, 0)
        inner_filter_widget.setLayout(inner_filter_layout)
        self._filter_widget = InfixCompletingLineEdit(parent=self)
        self._filter_widget.text_changed_debounced.connect(self._slot_filter_changed)
        self._completer_model = QStringListModel(self._filter_query_completions())
        completer = QtWidgets.QCompleter(self._completer_model, self)
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

    def hideEvent(self, _e: Any) -> None:
        self._update_timer.stop()

    def showEvent(self, _e: Any) -> None:
        if self._auto_refresh.isChecked():
            self._slot_refresh(force=True)
            self._update_timer.start(AUTO_REFRESH_TIMER_MSEC)

    def _retrieve_attributi_metadata(self, conn: Connection) -> List[TabledAttributo]:
        return [
            TabledAttributo(k, attributo)
            for k, attributi in self._db.retrieve_attributi(conn).items()
            for attributo in attributi.values()
        ]

    def _filter_query_completions(self) -> Iterable[str]:
        return [s.technical_id() for s in self._attributi_metadata]

    def _create_declarative_data(self) -> Data:
        def column_header(x: TabledAttributo) -> str:
            if isinstance(x.attributo.attributo_type, AttributoTypeDouble):
                if x.attributo.attributo_type.suffix:
                    return f"{x.pretty_id()} [{x.attributo.attributo_type.suffix}]"
            return x.pretty_id()

        columns = [
            Column(
                header_label=column_header(c),
                editable=False,
                header_callback=partial(self._header_menu_callback, c),
                sort_click_callback=partial(self._sort_clicked, c),
                sorted_by=self._sort_data[1]
                if self._sort_data[0].table == c.table
                and self._sort_data[0].attributo.name == c.attributo.name
                else None,
            )
            for c in self._visible_columns
        ]
        has_colors = self._has_colors()
        column_count = len(self._visible_columns)

        def row_colors(r: Dict[AssociatedTable, AttributiMap]) -> Dict[int, QBrush]:
            if not has_colors:
                return {}
            color = r[AssociatedTable.RUN].select_string(AttributoId("color"))
            if color is None:
                return {}
            real_color = name_to_hex(color)
            return {i: QBrush(QColor(real_color)) for i in range(column_count)}

        return Data(
            rows=[
                Row(
                    display_roles=[
                        pretty_print_attributo(
                            r.attributo,
                            c[r.table].select_value(r.attributo.name),
                            self._samples,
                        )
                        for r in self._visible_columns
                    ],
                    edit_roles=[
                        sortable_attributo(
                            r.attributo, c[r.table].select_value(r.attributo.name)
                        )
                        for r in self._visible_columns
                    ],
                    background_roles=row_colors(c),
                    change_callbacks=[],
                    double_click_callback=partial(self._double_click, c),
                    right_click_menu=partial(self._right_click_single, c),
                    key=c[AssociatedTable.RUN].select_int_unsafe(
                        self._db.tables.attributo_run_id
                    ),
                )
                for c in self._rows
                if self._row_not_filtered(c)
            ],
            columns=columns,
            row_delegates={},
            column_delegates={},
            global_right_click_callback=self._right_click_global,
        )

    def _has_colors(self) -> bool:
        return any(
            x.table == AssociatedTable.RUN and x.attributo.name == "color"
            for x in self._attributi_metadata
        )

    def _change_sample_for_multiple(
        self, run_ids: List[int], sample_id: Optional[int]
    ) -> None:
        logger.info(f"changing sample to {sample_id} for runs {run_ids}")
        with self._db.connect() as conn:
            for run_id in run_ids:
                self._db.update_run_attributo(
                    conn, run_id, AttributoId("sample_id"), sample_id
                )
        self._slot_refresh(force=True)

    def _change_color_for_multiple(
        self, run_ids: List[int], color: Optional[str]
    ) -> None:
        logger.info(f"changing color to {color} for runs {run_ids}")
        with self._db.connect() as conn:
            for run_id in run_ids:
                self._db.update_run_attributo(conn, run_id, AttributoId("color"), color)
        self._slot_refresh(force=True)

    def _change_sample_for_single(self, run_id: int, sample_id: Optional[int]) -> None:
        with self._db.connect() as conn:
            self._db.update_run_attributo(
                conn, run_id, AttributoId("sample_id"), sample_id
            )
        self._slot_refresh(force=True)

    def _change_color_for_single(self, run_id: int, color_name: Optional[str]) -> None:
        with self._db.connect() as conn:
            self._db.update_run_attributo(
                conn, run_id, AttributoId("color"), color_name
            )
        self._slot_refresh(force=True)

    def _right_click_global(self, run_ids: List[int], p: QPoint) -> None:
        menu = QMenu(self)

        change_sample_menu = menu.addMenu("Change sample for runs")
        for s in self._samples:
            sample_action = change_sample_menu.addAction(s.sample_name)
            sample_action.triggered.connect(
                partial(self._change_sample_for_multiple, run_ids, s.sample_id)
            )
        sample_none_action = change_sample_menu.addAction("None")
        sample_none_action.triggered.connect(
            partial(self._change_sample_for_multiple, run_ids, None)
        )

        change_color_menu = menu.addMenu("Change color for runs")
        for color_name in PREDEFINED_COLORS_TO_HEX:
            color_action = change_color_menu.addAction(color_name)
            color_action.triggered.connect(
                partial(self._change_color_for_multiple, run_ids, color_name)
            )
        color_none_action = change_color_menu.addAction("None")
        color_none_action.triggered.connect(
            partial(self._change_color_for_multiple, run_ids, None)
        )

        mass_change_action = menu.addAction(
            self.style().standardIcon(QStyle.SP_DialogResetButton),
            "Change attributi for runs",
        )

        deleteAction = menu.addAction(
            self.style().standardIcon(QStyle.SP_DialogCancelButton),
            "Delete runs",
        )

        action = menu.exec_(p)

        if action == deleteAction:
            with self._db.connect() as conn:
                if not self._check_password(
                    conn,
                    "Are you sure?",
                    "Really delete runs " + ", ".join(str(x) for x in run_ids) + "?",
                ):
                    return

                self._db.delete_runs(conn, run_ids)
                self._slot_refresh(force=True)
        elif action == mass_change_action:
            with self._db.connect() as conn:
                runs = [
                    r
                    for r in self._db.retrieve_runs(conn, self._proposal_id, since=None)
                    if r.id in run_ids
                ]

            changes = attributi_change_dialog(
                runs,
                {
                    k.attributo.name: k.attributo
                    for k in self._attributi_metadata
                    if k.table == AssociatedTable.RUN
                },
                self._samples,
                self,
            )

            if changes is not None:
                with self._db.connect() as conn:
                    apply_run_changes(self._db, conn, changes)
                self._slot_refresh(force=True)

    def _check_password(
        self, conn: Connection, headline: str, description: str
    ) -> bool:
        have_password = self._db.proposal_has_password(conn, self._proposal_id)

        if have_password:
            password = password_check_dialog(
                headline,
                description,
            )

            if not password:
                return False

            if not self._db.check_proposal_password(conn, self._proposal_id, password):
                QMessageBox.critical(  # type: ignore
                    self,
                    "Invalid password",
                    "Password was invalid!",
                    QMessageBox.Ok,
                )
                return False
            return True

        answer = QMessageBox.critical(
            self,
            headline,
            description,
            QMessageBox.Yes | QMessageBox.Cancel,
        )

        return answer == QMessageBox.Yes

    def _right_click_single(self, c: OverviewAttributi, p: QPoint) -> None:
        menu = QMenu(self)
        deleteAction = menu.addAction(
            self.style().standardIcon(QStyle.SP_DialogCancelButton),
            "Delete run",
        )
        run_id = c[AssociatedTable.RUN].select_int_unsafe(
            self._db.tables.attributo_run_id
        )
        change_sample_menu = menu.addMenu("Change sample")
        for s in self._samples:
            sample_action = change_sample_menu.addAction(s.sample_name)
            sample_action.triggered.connect(
                partial(self._change_sample_for_single, run_id, s.sample_id)
            )
        sample_none_action = change_sample_menu.addAction("None")
        sample_none_action.triggered.connect(
            partial(self._change_sample_for_single, run_id, None)
        )
        change_color_menu = menu.addMenu("Change color")
        for color_name in PREDEFINED_COLORS_TO_HEX:
            color_action = change_color_menu.addAction(color_name)
            color_action.triggered.connect(
                partial(self._change_color_for_single, run_id, color_name)
            )
        color_none_action = change_color_menu.addAction("None")
        color_none_action.triggered.connect(
            partial(self._change_color_for_single, run_id, None)
        )
        action = menu.exec_(p)
        if action == deleteAction:
            with self._db.connect() as conn:
                if not self._check_password(
                    conn,
                    f"Delete run “{run_id}”",
                    f"Are you sure you want to delete run “{run_id}”?",
                ):
                    return

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
            self._samples = self._db.retrieve_mini_samples(conn, self._proposal_id)
            self._attributi_metadata = self._retrieve_attributi_metadata(conn)
            if self._completer_model.stringList() != self._filter_query_completions():
                self._completer_model.setStringList(self._filter_query_completions())
            assert self._filter_widget.completer() is not None
            latest_update_time = self._db.overview_update_time(conn)
            latest_run_count = self._db.run_count(conn)
            needs_update = (
                latest_update_time is None
                or self._last_refresh is None
                or latest_update_time > self._last_refresh
            ) or latest_run_count != self._last_run_count
            if needs_update:
                self._rows = self._db.retrieve_overview(
                    conn, self._proposal_id, self._db.retrieve_attributi(conn)
                )
                self._last_refresh = latest_update_time
                self._last_run_count = latest_run_count
                self._rows.sort(
                    key=self._sort_key,
                    reverse=(self._sort_data[1] == SortOrder.DESC),
                )
            if needs_update or force:
                self._table_view.set_data(self._create_declarative_data())

    def _sort_key(self, k: OverviewAttributi) -> Tuple[bool, Any]:
        sort_column = self._sort_data[0]
        table_data = k[sort_column.table]
        if table_data is None:
            return True, None
        v = table_data.select_value(sort_column.attributo.name)
        result = sortable_attributo(sort_column.attributo, v)
        # We have to account for unset values while sorting.
        # This nifty solution I found on SO (where else):
        #
        # https://stackoverflow.com/questions/18411560/sort-list-while-pushing-none-values-to-the-end
        return result is None, result

    def _sort_clicked(self, column: TabledAttributo) -> None:
        self._sort_data = (
            column,
            self._sort_data[1].invert()
            if self._sort_data[0] == column
            else SortOrder.ASC,
        )
        self._rows.sort(
            key=self._sort_key, reverse=(self._sort_data[1] == SortOrder.DESC)
        )
        self._table_view.set_data(self._create_declarative_data())

    def _slot_plot_against(
        self, type_: _PlotType, y_axis: TabledAttributo, x_axis: TabledAttributo
    ) -> None:
        dialog = QtWidgets.QDialog(self)
        dialog_layout = QtWidgets.QVBoxLayout()
        dialog.setLayout(dialog_layout)

        plot_dialog = PlotDialog(
            self._db,
            self._proposal_id,
            self._filter_query,
            type_=type_,
            x_axis=x_axis,
            y_axis=y_axis,
            parent=self,
        )
        plot_dialog.show()

    def _header_menu_callback(self, column: TabledAttributo, pos: QPoint) -> None:
        if column.attributo.attributo_type is None:
            return
        if not _plottable_attributo_type(column.attributo.attributo_type):
            return
        menu = QtWidgets.QMenu(self)
        line_plot_against_menu = menu.addMenu("Line plot against…")
        scatter_plot_against_menu = menu.addMenu("Scatter plot against…")
        for attributo in (
            a
            for a in self._attributi_metadata
            if _plottable_attributo_type(a.attributo.attributo_type)
        ):
            new_line_action = line_plot_against_menu.addAction(attributo.pretty_id())
            new_scatter_action = scatter_plot_against_menu.addAction(
                attributo.pretty_id()
            )
            if (
                attributo.table == AssociatedTable.RUN
                and attributo.attributo.name == self._db.tables.attributo_run_id
            ):
                f = new_line_action.font()
                f.setBold(True)
                new_line_action.setFont(f)
                f = new_scatter_action.font()
                f.setBold(True)
                new_scatter_action.setFont(f)
            new_line_action.triggered.connect(
                partial(self._slot_plot_against, _PlotType.LINE, column, attributo)
            )
            new_scatter_action.triggered.connect(
                partial(self._slot_plot_against, _PlotType.SCATTER, column, attributo)
            )
        menu.exec_(pos)

    def run_changed(self) -> None:
        self._slot_refresh()

    def _slot_switch_columns(self) -> None:
        new_columns = display_column_chooser(
            self, self._visible_columns, self._attributi_metadata
        )
        self._visible_columns = new_columns
        self._visible_columns.sort(key=_attributo_sort_key)
        self._slot_refresh(force=True)

    def _late_init(self) -> None:
        self._slot_refresh()

    def _slot_filter_changed(self, f: str) -> None:
        logger.info('filter change to "%s"', f)
        try:
            self._filter_query = parse_query(
                f, set(k.technical_id() for k in self._attributi_metadata)
            )
            logger.info("Filter query changed successfully, refreshing")
            self._query_error.setText("")
        except UnexpectedEOF:
            self._query_error.setText("")
        except Exception as e:
            self._query_error.setText(f"{e}")
        self._slot_refresh(force=True)
