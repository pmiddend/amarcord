from typing import Optional
from typing import Dict
from typing import Final
from typing import Any
from typing import List
from typing import Set
import logging
import pandas as pd
from PyQt5 import QtWidgets
from PyQt5 import QtCore
from matplotlib.backends.backend_qt5agg import (
    FigureCanvasQTAgg,
    NavigationToolbar2QT as NavigationToolbar,
)
from matplotlib.figure import Figure

from amarcord.modules.spb.run_property import (
    RunProperty,
    default_visible_properties,
    run_property_name,
    unplottable_properties,
)
from amarcord.modules.spb.queries import SPBQueries, Comment
from amarcord.modules.spb.proposal_id import ProposalId
from amarcord.modules.context import Context
from amarcord.modules.spb.tables import Tables
from amarcord.qt.infix_completer import InfixCompletingLineEdit
from amarcord.qt.table import GeneralTableWidget
from amarcord.query_parser import parse_query
from amarcord.query_parser import UnexpectedEOF

logger = logging.getLogger(__name__)


def _column_query_names() -> Set[str]:
    return {f.name.lower() for f in RunProperty}


Row = Dict[RunProperty, Any]


def _retrieve_data_no_connection(db: SPBQueries, proposal_id: ProposalId) -> List[Row]:
    with db.dbcontext.connect() as conn:
        return db.retrieve_runs(conn, proposal_id)


def _convert_tag_column(value: Set[str], role: int) -> Any:
    if role == QtCore.Qt.DisplayRole:
        return ", ".join(value)
    if role == QtCore.Qt.EditRole:
        return value
    return QtCore.QVariant()


def _convert_comment_column(comments: List[Comment], role: int) -> Any:
    if role == QtCore.Qt.DisplayRole:
        return "\n".join(f"{c.author}: {c.text}" for c in comments)
    if role == QtCore.Qt.EditRole:
        return comments
    return QtCore.QVariant()


_column_converters: Final = {
    RunProperty.TAGS: _convert_tag_column,
    RunProperty.COMMENTS: _convert_comment_column,
}


def _display_column_chooser(
    parent: Optional[QtWidgets.QWidget], selected_columns: List[RunProperty]
) -> List[RunProperty]:
    dialog = QtWidgets.QDialog(parent)
    dialog_layout = QtWidgets.QVBoxLayout()
    dialog.setLayout(dialog_layout)
    root_widget = QtWidgets.QGroupBox("Choose which columns to display:")
    dialog_layout.addWidget(root_widget)
    root_layout = QtWidgets.QVBoxLayout()
    root_widget.setLayout(root_layout)
    column_list = QtWidgets.QListWidget()
    column_list.setSelectionMode(QtWidgets.QAbstractItemView.ExtendedSelection)
    for col in RunProperty:
        new_item = QtWidgets.QListWidgetItem(run_property_name(col))
        new_item.setData(QtCore.Qt.UserRole, col.value)
        column_list.addItem(new_item)
    for col in selected_columns:
        # -1 here because auto lets the enum start at 1 (which is fine actually)
        column_list.selectionModel().select(
            column_list.model().index(col.value - 1, 0),
            # pylint: disable=no-member
            QtCore.QItemSelectionModel.SelectionFlag.Select,  # type: ignore
        )
    root_layout.addWidget(column_list)
    buttonBox = QtWidgets.QDialogButtonBox(  # type: ignore
        QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel
    )
    buttonBox.accepted.connect(dialog.accept)
    buttonBox.rejected.connect(dialog.reject)
    root_layout.addWidget(buttonBox)

    def selection_changed(
        selected: QtCore.QItemSelection, deselected: QtCore.QItemSelection
    ) -> None:
        # pylint: disable=no-member
        buttonBox.button(QtWidgets.QDialogButtonBox.StandardButton.Ok).setEnabled(  # type: ignore
            bool(column_list.selectedItems())
        )

    column_list.selectionModel().selectionChanged.connect(selection_changed)  # type: ignore
    if dialog.exec() == QtWidgets.QDialog.Rejected:
        return selected_columns
    return [
        RunProperty(k.data(QtCore.Qt.UserRole)) for k in column_list.selectedItems()
    ]


class _MplCanvas(FigureCanvasQTAgg):
    def __init__(self, parent=None, width=5, height=4, dpi=100):
        fig = Figure(figsize=(width, height), dpi=dpi)
        self.axes = fig.add_subplot(111)
        super().__init__(fig)


class RunTable(QtWidgets.QWidget):
    run_selected = QtCore.pyqtSignal(int)

    def __init__(
        self, context: Context, tables: Tables, proposal_id: ProposalId
    ) -> None:
        super().__init__()

        self._proposal_id = proposal_id
        self._db = SPBQueries(context.db, tables)

        # Init main widgets
        choose_columns = QtWidgets.QPushButton("Choose columns")
        choose_columns.setIcon(
            self.style().standardIcon(QtWidgets.QStyle.SP_FileDialogDetailedView)
        )
        choose_columns.clicked.connect(self._slot_switch_columns)

        self._context = context
        self._table_view = GeneralTableWidget[RunProperty](
            RunProperty,
            column_headers={c: run_property_name(c) for c in RunProperty},
            column_visibility=default_visible_properties,
            column_converters=_column_converters,
            data_retriever=None,
            parent=self,
        )
        self._table_view.set_menu_callback(self._header_menu_callback)
        self._table_view.row_double_click.connect(self._slot_row_selected)

        log_output = QtWidgets.QPlainTextEdit()
        log_output.setReadOnly(True)

        # Layouting stuff
        root_layout = QtWidgets.QVBoxLayout(self)
        header_layout = QtWidgets.QHBoxLayout()
        header_layout.addWidget(choose_columns, 0, QtCore.Qt.AlignTop)
        header_layout.addWidget(
            QtWidgets.QLabel("Filter query:"), 0, QtCore.Qt.AlignTop
        )
        filter_widget = InfixCompletingLineEdit(self)
        filter_widget.textChanged.connect(self._slot_filter_changed)
        completer = QtWidgets.QCompleter(list(_column_query_names()), self)
        completer.setCompletionMode(QtWidgets.QCompleter.InlineCompletion)
        filter_widget.setCompleter(completer)

        query_layout = QtWidgets.QVBoxLayout()
        # filter_widget = QtWidgets.QLineEdit()
        query_layout.addWidget(filter_widget)
        self._query_error = QtWidgets.QLabel("")
        self._query_error.setStyleSheet("QLabel { font: italic 10px; color: red; }")
        query_layout.addWidget(self._query_error)
        query_layout.setSpacing(0)
        query_layout.setContentsMargins(0, 0, 0, 0)

        header_layout.addLayout(query_layout)

        root_layout.addLayout(header_layout)
        root_layout.addWidget(
            QtWidgets.QLabel(
                "<b>Double-click</b> row to view details. <b>Right-click</b> column header to show options."
            )
        )

        root_layout.addWidget(self._table_view)
        context.db.after_db_created(self._late_init)

    def _header_menu_callback(self, pos: QtCore.QPoint, column: RunProperty) -> None:
        if column in unplottable_properties:
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

            sc = _MplCanvas(self, width=5, height=4, dpi=100)

            df = pd.DataFrame(
                self._table_view.get_filtered_column_values(column),
                index=self._table_view.get_filtered_column_values(RunProperty.STARTED),
                columns=[run_property_name(column)],
            )

            df.plot(ax=sc.axes)

            # Create toolbar, passing canvas as first parament, parent (self, the MainWindow) as second.
            toolbar = NavigationToolbar(sc, self)

            dialog_layout.addWidget(toolbar)
            dialog_layout.addWidget(sc)
            button_box = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Close)
            button_box.rejected.connect(dialog.reject)
            dialog_layout.addWidget(button_box)

            dialog.exec()

    def _slot_row_selected(self, row: Dict[RunProperty, Any]) -> None:
        self.run_selected.emit(row[RunProperty.RUN_ID])

    def run_changed(self) -> None:
        logger.info("Refreshing run table")
        self._table_view.refresh()

    def _slot_switch_columns(self) -> None:
        new_columns = _display_column_chooser(self, self._table_view.column_visibility)
        self._table_view.set_column_visibility(new_columns)

    def _late_init(self) -> None:
        logger.info("Late initing")

        self._table_view.set_data_retriever(
            lambda: _retrieve_data_no_connection(self._db, self._proposal_id)  # type: ignore
        )

    def _slot_filter_changed(self, f: str) -> None:
        try:
            query = parse_query(f, _column_query_names())
            self._table_view.set_filter_query(query)
        except UnexpectedEOF:
            self._query_error.setText("")
        except Exception as e:
            self._query_error.setText(f"{e}")
