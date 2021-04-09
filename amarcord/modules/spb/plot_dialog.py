import datetime
import logging
from enum import Enum
from enum import auto
from time import sleep
from typing import Final
from typing import List
from typing import Optional

import pandas as pd
from PyQt5.QtCore import QObject
from PyQt5.QtCore import QThread
from PyQt5.QtCore import QVariant
from PyQt5.QtCore import pyqtSignal
from PyQt5.QtWidgets import QDialog
from PyQt5.QtWidgets import QDialogButtonBox
from PyQt5.QtWidgets import QVBoxLayout
from PyQt5.QtWidgets import QWidget
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg
from matplotlib.backends.backend_qt5agg import NavigationToolbar2QT as NavigationToolbar
from matplotlib.pyplot import Figure

from amarcord.db.db import Connection
from amarcord.db.db import DB
from amarcord.db.db import OverviewAttributi
from amarcord.db.db import overview_row_to_query_row
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


class _PlotType(Enum):
    LINE = auto()
    SCATTER = auto()


class _Worker(QObject):
    finished = pyqtSignal(QVariant, QVariant)

    def __init__(
        self, db: DB, proposal_id: ProposalId, last_update: Optional[datetime.datetime]
    ) -> None:
        super().__init__()
        self._db = db
        self._proposal_id = proposal_id
        self._last_update = last_update

    def run(self) -> None:
        with self._db.connect() as conn:
            while True:
                sleep(TIMER_INTERVAL_MSEC / 1000)
                update_time = self._db.overview_update_time(conn)
                if (
                    update_time is None
                    or self._last_update is None
                    or update_time > self._last_update
                ):
                    self._last_update = datetime.datetime.utcnow()
                    rows = self._db.retrieve_overview(
                        conn, self._proposal_id, self._db.retrieve_attributi(conn)
                    )
                    attributi_metadata = _retrieve_attributi_metadata(conn, self._db)

                    self.finished.emit(rows, attributi_metadata)


def _retrieve_attributi_metadata(conn: Connection, db: DB) -> List[TabledAttributo]:
    return [
        TabledAttributo(k, attributo)
        for k, attributi in db.retrieve_attributi(conn).items()
        for attributo in attributi.values()
    ]


class PlotDialog(QDialog):
    def __init__(
        self,
        db: DB,
        proposal_id: ProposalId,
        filter_query: Query,
        type_: _PlotType,
        x_axis: TabledAttributo,
        y_axis: TabledAttributo,
        parent: Optional[QWidget] = None,
    ) -> None:
        super().__init__(parent)

        self._db = db
        self._filter_query = filter_query
        self._type = type_
        self._x_axis = x_axis
        self._y_axis = y_axis
        self._proposal_id = proposal_id
        self.setWindowTitle(f"Plot {x_axis.pretty_id()} vs {y_axis.pretty_id()}")

        with self._db.connect() as conn:
            self._last_update: Optional[datetime.datetime] = datetime.datetime.utcnow()
            db = self._db
            self._attributi_metadata = _retrieve_attributi_metadata(conn, db)
            self._rows = self._db.retrieve_overview(
                conn, proposal_id, self._db.retrieve_attributi(conn)
            )

        dialog_layout = QVBoxLayout(self)

        self._sc = _MplCanvas(width=5, height=4, dpi=100)

        toolbar = NavigationToolbar(self._sc, self)

        dialog_layout.addWidget(self._sc)
        # filter_line = QHBoxLayout()
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

        self._plot()

        self._worker = _Worker(self._db, self._proposal_id, self._last_update)
        self._thread = QThread()
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)  # type: ignore
        self._thread.finished.connect(self._thread.deleteLater)  # type: ignore
        self._worker.finished.connect(self._update)
        self._thread.start()

    def _update(
        self, rows: List[OverviewAttributi], attributi: List[TabledAttributo]
    ) -> None:
        self._rows = rows
        self._attributi_metadata = attributi
        self._df = self._create_data_frame()

        self._sc.axes.clear()

        self._plot()
        self._sc.draw()

    def _plot(self):
        try:
            if self._type == _PlotType.LINE:
                # noinspection PyArgumentList
                self._df.plot(ax=self._sc.axes, legend=None)  # type: ignore
                self._sc.axes.set_xlabel(self._y_axis.pretty_id())
                self._sc.axes.set_ylabel(self._x_axis.pretty_id())
            else:
                # noinspection PyArgumentList
                self._df.plot(ax=self._sc.axes, x=0, y=1, kind="scatter")  # type: ignore
        except TypeError:
            # this happens if we have a column consisting only of None values, for example.
            # We can ignore this for now, and wait for a successful refresh below.
            pass

    def _create_data_frame(self) -> pd.DataFrame:
        filtered_rows = [
            r
            for r in self._rows
            if self._filter_query(
                overview_row_to_query_row(r, self._attributi_metadata)
            )
        ]
        if self._type == _PlotType.LINE:
            return pd.DataFrame(
                [
                    r[self._x_axis.table].select_value(self._x_axis.attributo.name)
                    for r in filtered_rows
                ],
                index=[
                    r[self._y_axis.table].select_value(self._y_axis.attributo.name)
                    for r in filtered_rows
                ],
                columns=[self._x_axis.attributo.pretty_id()],
            )
        return pd.DataFrame(
            [
                [
                    r[self._x_axis.table].select_value(self._x_axis.attributo.name),
                    r[self._y_axis.table].select_value(self._y_axis.attributo.name),
                ]
                for r in filtered_rows
            ],
            columns=[
                self._x_axis.attributo.pretty_id(),
                self._y_axis.attributo.pretty_id(),
            ],
        )
