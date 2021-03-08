from typing import Final, Optional
import datetime
import logging
import pandas as pd
from PyQt5.QtCore import QTimer

from PyQt5.QtWidgets import (
    QCompleter,
    QDialog,
    QDialogButtonBox,
    QHBoxLayout,
    QLabel,
    QVBoxLayout,
    QWidget,
)
from matplotlib.backends.backend_qt5agg import (
    FigureCanvasQTAgg,
    NavigationToolbar2QT as NavigationToolbar,
)
from matplotlib.pyplot import Figure

from amarcord.modules.spb.db import DB
from amarcord.modules.spb.proposal_id import ProposalId
from amarcord.modules.spb.attributo_id import AttributoId
from amarcord.qt.infix_completer import InfixCompletingLineEdit
from amarcord.query_parser import Query, filter_by_query, parse_query

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
        filter_query: str,
        property_: AttributoId,
        parent: Optional[QWidget] = None,
    ) -> None:
        super().__init__(parent)

        self._db = db
        self._filter_query = filter_query
        self._property = property_
        self._proposal_id = proposal_id

        with self._db.connect() as conn:
            self._runs = self._db.retrieve_runs(conn, proposal_id, None)
            self._property_metadata = self._db.run_property_metadata(conn)

        dialog_layout = QVBoxLayout()
        self.setLayout(dialog_layout)

        self._sc = _MplCanvas(width=5, height=4, dpi=100)
        self._last_update = datetime.datetime.utcnow()

        toolbar = NavigationToolbar(self._sc, self)

        dialog_layout.addWidget(self._sc)
        filter_line = QHBoxLayout()
        dialog_layout.addLayout(filter_line)
        dialog_layout.addWidget(toolbar)
        self._filter_widget = InfixCompletingLineEdit(
            content=self._filter_query, parent=self
        )
        self._filter_widget.textChanged.connect(self._slot_filter_changed)
        self._completer = QCompleter(
            list(str(s) for s in self._property_metadata.keys()), self
        )
        self._completer.setCompletionMode(QCompleter.InlineCompletion)
        self._filter_widget.setCompleter(self._completer)
        filter_line.addWidget(QLabel("Query:"))
        filter_line.addWidget(self._filter_widget)
        button_box = QDialogButtonBox(QDialogButtonBox.Close)
        button_box.rejected.connect(self.reject)
        dialog_layout.addWidget(button_box)

        self._query: Optional[Query] = None
        self._update_query()
        if self._query is not None:
            filtered_runs = [
                r
                for r in self._runs
                if property_ in r and filter_by_query(self._query, r)
            ]
            self._df = pd.DataFrame(
                [r[self._property] for r in filtered_runs],
                index=[
                    datetime.datetime.fromisoformat(r[AttributoId("started")])
                    for r in filtered_runs
                ],
                columns=[self._property_metadata[property_].description],
            )

            self._sc.axes.clear()
            # noinspection PyArgumentList
            self._df.plot(ax=self._sc.axes)  # type: ignore

        self._timer = QTimer(self)
        self._timer.timeout.connect(self._slot_refresh)
        self._timer.start(TIMER_INTERVAL_MSEC)

    def _update_query(self):
        try:
            self._query: Optional[Query] = parse_query(
                self._filter_widget.text(),
                set(str(s) for s in self._property_metadata.keys()),
            )
        except:
            self._query = None

    def _slot_filter_changed(self, new_text: str) -> None:
        self._filter_query = new_text
        self._slot_refresh()

    def _slot_refresh(self) -> None:
        self._update_query()

        if self._query is None:
            return

        with self._db.connect() as conn:
            self._runs = self._db.retrieve_runs(conn, self._proposal_id, None)
            self._last_update = datetime.datetime.utcnow()
            filtered_runs = [
                r
                for r in self._runs
                if self._property in r and filter_by_query(self._query, r)
            ]
            self._df = pd.DataFrame(
                [r[self._property] for r in filtered_runs],
                index=[
                    datetime.datetime.fromisoformat(r[AttributoId("started")])
                    for r in filtered_runs
                ],
                columns=[self._property_metadata[self._property].description],
            )

            self._sc.axes.clear()

            if filtered_runs:
                # noinspection PyArgumentList
                self._df.plot(ax=self._sc.axes)  # type: ignore
                self._sc.draw()
