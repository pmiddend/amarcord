from typing import Optional

from PyQt5.QtWidgets import QHBoxLayout
from PyQt5.QtWidgets import QWidget

from amarcord.db.db import DB
from amarcord.db.db import DBAnalysisResults
from amarcord.db.tables import DBTables
from amarcord.modules.context import Context
from amarcord.qt.declarative_table import Column
from amarcord.qt.declarative_table import Data
from amarcord.qt.declarative_table import DeclarativeTable
from amarcord.qt.declarative_table import Row
from amarcord.qt.declarative_table import SortOrder


def _create_row(r: DBAnalysisResults) -> Row:
    max_hit_rate = max(r.hit_rates, default=None)
    max_rsplit = max(r.rsplits, default=None)
    max_cc_half = max(r.cc_halfs, default=None)
    max_indexed = max(r.num_indexed, default=None)
    return Row(
        display_roles=[
            str(r.sample_id),
            f"{min(r.run_ids)}-{max(r.run_ids)}" if r.run_ids else "",
            f"{max_hit_rate:.2f}%" if max_hit_rate is not None else "",
            ",".join(str(nc) for nc in r.num_crystals),
            str(max_indexed) if max_indexed is not None else "",
            f"{max_rsplit:.2f}%" if max_rsplit is not None else "",
            f"{max_cc_half:.2f}%" if max_cc_half is not None else "",
        ],
        edit_roles=[
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ],
    )


class AnalysisView(QWidget):
    def __init__(
        self,
        context: Context,
        tables: DBTables,
        parent: Optional[QWidget] = None,
    ) -> None:
        super().__init__(parent)

        self._db = DB(context.db, tables)

        root_layout = QHBoxLayout(self)

        self._table = DeclarativeTable(data=self._create_data())

        root_layout.addWidget(self._table)

    def _create_data(self) -> Data:
        with self._db.connect() as conn:
            return Data(
                columns=[
                    Column(
                        header_label="Sample ID",
                        editable=False,
                        sorted_by=SortOrder.ASC,
                    ),
                    Column(header_label="Runs", editable=False, stretch=True),
                    Column(header_label="Maximum hit rate", editable=False),
                    Column(header_label="Indexed Crystals", editable=False),
                    Column(header_label="Max Indexed", editable=False),
                    Column(header_label="Max Rₛₚₗᵢₜ", editable=False),
                    Column(header_label="Max CC½", editable=False),
                ],
                rows=[_create_row(r) for r in self._db.retrieve_analysis(conn)],
                row_delegates={},
                column_delegates={},
            )
