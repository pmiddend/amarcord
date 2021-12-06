import datetime
import logging
from functools import partial
from typing import Callable
from typing import Dict
from typing import Final
from typing import Iterable
from typing import List
from typing import Optional
from typing import Set
from typing import cast

from PyQt5 import QtWidgets
from PyQt5.QtCore import pyqtSignal
from PyQt5.QtGui import QBrush
from PyQt5.QtGui import QColor
from PyQt5.QtWidgets import QHBoxLayout
from PyQt5.QtWidgets import QLabel
from PyQt5.QtWidgets import QPushButton
from PyQt5.QtWidgets import QVBoxLayout
from PyQt5.QtWidgets import QWidget

from amarcord.db.analysis_result import DBAnalysisResult
from amarcord.db.attributo_id import AttributoId
from amarcord.db.db import DB
from amarcord.db.proposal_id import ProposalId
from amarcord.db.table_classes import DBRun
from amarcord.db.table_classes import DBSample
from amarcord.db.tables import DBTables
from amarcord.modules.context import Context
from amarcord.modules.spb.colors import name_to_hex
from amarcord.qt.declarative_table import Column
from amarcord.qt.declarative_table import Data
from amarcord.qt.declarative_table import DeclarativeTable
from amarcord.qt.declarative_table import Row

NEW_SAMPLE_HEADLINE = "New sample"
AUTO_REFRESH_TIMER_MSEC: Final = 5000

logger = logging.getLogger(__name__)


def analysis_results_to_table_data(
    ar: Iterable[DBAnalysisResult],
    runs: List[DBRun],
    samples: Iterable[DBSample],
    comment_changed: Callable[[str, str], None],
    parent: QWidget,
) -> Data:
    columns = [
        "Runs",
        "Sample",
        "Resolution",
        "Rsplit [%]",
        "CC(1/2)",
        "CC*",
        "SNR",
        "Completeness [%]",
        "Multiplicity",
        "No. measurements",
        "Unique reflections",
        "Wilson B-factor",
        "No. Patterns",
        "No. Hits",
        "Idx. Patterns",
        "Idx. Crystals",
        "Outer shell",
    ]

    sample_id_to_sample_name: Dict[int, str] = {
        cast(int, s.id): s.name for s in samples
    }
    run_to_sample_name: Dict[int, Optional[str]] = {
        r.id: sample_id_to_sample_name.get(r.sample_id, None)
        if r.sample_id is not None
        else None
        for r in runs
    }

    def sample_names_for_runs(run_from: int, run_to: int) -> Set[str]:
        sample_names = set(
            run_to_sample_name.get(run_id, None)
            for run_id in range(run_from, run_to + 1)
        )
        sample_names.discard(None)
        return cast(Set[str], sample_names)

    def make_row(r: DBAnalysisResult) -> Row:

        display_roles = [
            str(r.run_from) if r.run_to == r.run_from else f"{r.run_from}-{r.run_to}",
            ", ".join(sample_names_for_runs(r.run_from, r.run_to)),
            r.resolution,
            f"{r.rsplit:.3f}",
            f"{r.cchalf:.3f}",
            f"{r.ccstar:.3f}",
            f"{r.snr:.3f}",
            f"{r.completeness:.3f}",
            f"{r.multiplicity:.3f}",
            str(r.total_measurements),
            str(r.unique_reflections),
            f"{r.wilson_b:.3f}",
            str(r.num_patterns),
            str(r.num_hits),
            str(r.indexed_patterns),
            str(r.indexed_crystals),
            r.outer_shell,
            r.comment,
        ]

        first_run = next(iter(run for run in runs if run.id == r.run_from), None)

        row_colors: Dict[int, QBrush] = {}
        if first_run is not None:
            run_color = first_run.attributi.select_string(AttributoId("color"))
            if run_color is not None:
                real_color = name_to_hex(run_color)
                row_colors = {
                    i: QBrush(QColor(real_color)) for i in range(len(display_roles))
                }

        return Row(
            display_roles=display_roles,
            edit_roles=[None for _ in columns] + [r.comment],  # type: ignore
            background_roles=row_colors,
            change_callbacks=[None for _ in columns]  # type: ignore
            + [partial(comment_changed, r.directory_name)],  # type: ignore
        )

    return Data(
        rows=[make_row(r) for r in sorted(ar, key=lambda result: result.run_from)],
        columns=(
            [Column(c, editable=False) for c in columns]
            + [Column("Comment [double-click to edit]", editable=True, stretch=True)]
        ),
        row_delegates={},
        column_delegates={len(columns): QtWidgets.QStyledItemDelegate(parent=parent)},
    )


class AnalysisResultsTab(QWidget):
    new_attributo = pyqtSignal()

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
        root_layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(root_layout)

        root_widget = QWidget(self)
        top_layout = QVBoxLayout()
        root_widget.setLayout(top_layout)

        root_layout.addWidget(root_widget)

        refresh_button = QPushButton(
            self.style().standardIcon(QtWidgets.QStyle.SP_BrowserReload),
            "Refresh",
        )
        refresh_button.clicked.connect(self._slot_refresh)
        button_layout = QHBoxLayout()
        top_layout.addLayout(button_layout)
        button_layout.addWidget(refresh_button)
        self._last_refresh = datetime.datetime.now()
        self._last_refresh_label = QLabel(self.last_refresh_text())
        button_layout.addWidget(self._last_refresh_label)
        button_layout.addStretch()

        with self._db.connect() as conn:
            self._table = DeclarativeTable(
                analysis_results_to_table_data(
                    self._db.retrieve_analysis_results(conn),
                    self._db.retrieve_runs(conn, self._proposal_id, since=None),
                    self._db.retrieve_samples(conn, self._proposal_id),
                    self._comment_changed,
                    self,
                ),
            )
            top_layout.addWidget(self._table)

    def last_refresh_text(self):
        return "Last refresh: " + self._last_refresh.strftime("%H:%M:%S")

    def _comment_changed(self, directory_name: str, new_comment: str) -> None:
        with self._db.connect() as conn:
            self._db.change_analysis_result_comment(conn, directory_name, new_comment)
        self._slot_refresh()

    def _slot_refresh(self) -> None:
        with self._db.connect() as conn:
            self._table.set_data(
                analysis_results_to_table_data(
                    self._db.retrieve_analysis_results(conn),
                    self._db.retrieve_runs(conn, self._proposal_id, since=None),
                    self._db.retrieve_samples(conn, self._proposal_id),
                    self._comment_changed,
                    self,
                )
            )
            self._last_refresh = datetime.datetime.now()
            self._last_refresh_label.setText(self.last_refresh_text())
