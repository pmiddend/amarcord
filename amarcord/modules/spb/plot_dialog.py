import datetime
import logging
from typing import Final, List, Optional

import pandas as pd
from PyQt5.QtCore import QTimer
from PyQt5.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QHBoxLayout,
    QVBoxLayout,
    QWidget,
)
from matplotlib.backends.backend_qt5agg import (
    FigureCanvasQTAgg,
    NavigationToolbar2QT as NavigationToolbar,
)
from matplotlib.pyplot import Figure

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributo_id import AttributoId
from amarcord.db.db import Connection, DB, overview_row_to_query_row
from amarcord.db.proposal_id import ProposalId
from amarcord.db.tabled_attributo import TabledAttributo
from amarcord.query_parser import Query

TIMER_INTERVAL_MSEC: Final = 1000

logger = logging.getLogger(__name__)


class _MplCanvas(FigureCanvasQTAgg):
    def __init__(self, width=5, height=4, dpi=100):
        fig = Figure(figsize=(width, height), dpi=dpi)
        self.axes = fig.add_subplot(111)
        super().__init__(fig)


class PlotDialog(QDialog):
    def __init__(
        self,
        db: DB,
        proposal_id: ProposalId,
        filter_query: Query,
        attributo: TabledAttributo,
        parent: Optional[QWidget] = None,
    ) -> None:
        super().__init__(parent)

        self._db = db
        self._filter_query = filter_query
        self._attributo = attributo
        self._proposal_id = proposal_id

        with self._db.connect() as conn:
            self._last_update = datetime.datetime.utcnow()
            self._attributi_metadata = self._retrieve_attributi_metadata(conn)
            self._rows = self._db.retrieve_overview(
                conn, proposal_id, self._db.retrieve_attributi(conn)
            )

        dialog_layout = QVBoxLayout(self)

        self._sc = _MplCanvas(width=5, height=4, dpi=100)

        toolbar = NavigationToolbar(self._sc, self)

        dialog_layout.addWidget(self._sc)
        filter_line = QHBoxLayout()
        dialog_layout.addWidget(toolbar)
        # dialog_layout.addLayout(filter_line)
        # self._filter_widget = InfixCompletingLineEdit(
        #     content=self._filter_query, parent=self
        # )
        # self._filter_widget.textChanged.connect(self._slot_filter_changed)
        # self._completer = QCompleter(self._filter_query_completions(), self)
        # self._completer.setCompletionMode(QCompleter.InlineCompletion)
        # self._filter_widget.setCompleter(self._completer)
        # filter_line.addWidget(QLabel("Query:"))
        # filter_line.addWidget(self._filter_widget)
        button_box = QDialogButtonBox(QDialogButtonBox.Close)
        button_box.rejected.connect(self.reject)
        dialog_layout.addWidget(button_box)

        # self._query: Optional[Query] = None
        # self._update_query()
        # if self._query is not None:
        #     filtered_runs = [
        #         r
        #         for r in self._rows
        #         if r.select(attributo) is not None
        #         and filter_by_query(
        #             self._query, r.to_query_row(self._property_metadata.keys())
        #         )
        #     ]
        self._df = self._create_data_frame()

        self._sc.axes.clear()
        # noinspection PyArgumentList
        self._df.plot(ax=self._sc.axes)  # type: ignore

        self._timer = QTimer(self)
        self._timer.timeout.connect(self._slot_refresh)
        self._timer.start(TIMER_INTERVAL_MSEC)

    def _create_data_frame(self):
        filtered_rows = [
            r
            for r in self._rows
            if self._filter_query(
                overview_row_to_query_row(r, self._attributi_metadata)
            )
        ]
        return pd.DataFrame(
            [
                r[self._attributo.table].select_value(self._attributo.attributo.name)
                for r in filtered_rows
            ],
            index=[
                r[AssociatedTable.RUN].select_datetime(AttributoId("started"))
                for r in filtered_rows
            ],
            columns=[self._attributo.attributo.pretty_id()],
        )

    def _retrieve_attributi_metadata(self, conn: Connection) -> List[TabledAttributo]:
        return [
            TabledAttributo(k, attributo)
            for k, attributi in self._db.retrieve_attributi(conn).items()
            for attributo in attributi.values()
        ]

    # def _update_query(self):
    #     try:
    #         self._query: Optional[Query] = parse_query(
    #             self._filter_widget.text(),
    #             set(str(s) for s in self._property_metadata.keys()),
    #         )
    #     except:
    #         self._query = None

    def _slot_filter_changed(self, new_text: str) -> None:
        self._filter_query = new_text
        self._slot_refresh()

    def _slot_refresh(self) -> None:
        # self._update_query()
        #
        # if self._query is None:
        #     return
        #
        with self._db.connect() as conn:
            update_time = self._db.overview_update_time(conn)
            if update_time > self._last_update:
                self._last_update = datetime.datetime.utcnow()
                self._rows = self._db.retrieve_overview(
                    conn, self._proposal_id, self._db.retrieve_attributi(conn)
                )
                self._attributi_metadata = self._retrieve_attributi_metadata(conn)
                # filtered_runs = [
                #     r
                #     for r in self._rows
                #     if r.select(self._attributo)
                #     and filter_by_query(
                #         self._query, r.to_query_row(self._property_metadata.keys())
                #     )
                # ]
                self._df = self._create_data_frame()

                self._sc.axes.clear()

                # if filtered_runs:
                # noinspection PyArgumentList
                self._df.plot(ax=self._sc.axes)  # type: ignore
                self._sc.draw()
