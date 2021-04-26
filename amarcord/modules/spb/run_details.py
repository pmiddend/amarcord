import logging
from typing import Any
from typing import Dict
from typing import Optional
from typing import cast

from PyQt5 import QtWidgets
from PyQt5.QtCore import Qt
from PyQt5.QtCore import pyqtSignal
from PyQt5.QtWidgets import QLabel
from PyQt5.QtWidgets import QPushButton
from PyQt5.QtWidgets import QSizePolicy
from PyQt5.QtWidgets import QStyle
from PyQt5.QtWidgets import QWidget

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoTypeSample
from amarcord.db.comment import DBComment
from amarcord.db.db import Connection
from amarcord.db.db import DB
from amarcord.db.db import RunNotFound
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.karabo import Karabo
from amarcord.db.proposal_id import ProposalId
from amarcord.db.raw_attributi_map import RawAttributiMap
from amarcord.db.table_classes import DBRun
from amarcord.db.tables import DBTables
from amarcord.modules.context import Context
from amarcord.modules.spb.new_run_dialog import new_run_dialog
from amarcord.modules.spb.run_details_inner import RunDetailsInner

logger = logging.getLogger(__name__)


def _refresh_button(style: QStyle) -> QPushButton:
    return QPushButton(
        style.standardIcon(QtWidgets.QStyle.SP_BrowserReload),
        "Refresh",
    )


class RunDetails(QWidget):
    run_changed = pyqtSignal()
    new_attributo = pyqtSignal()

    def __init__(
        self, context: Context, tables: DBTables, proposal_id: ProposalId
    ) -> None:
        super().__init__()

        self._filter_fields = {
            tables.attributo_run_comments,
            tables.attributo_run_modified,
            tables.attributo_run_proposal_id,
            tables.attributo_run_id,
        }

        self._proposal_id = proposal_id
        self._context = context
        self._db = DB(context.db, tables)

        self._root_layout = QtWidgets.QVBoxLayout(self)
        self._inner: Optional[RunDetailsInner] = None
        self._empty_gui_present = False

        self._refresh_run_ids()

    def select_run(self, new_run_id: int) -> None:
        self._slot_current_run_changed(new_run_id)

    def _timed_refresh(self) -> None:
        if self._inner is not None:
            self._slot_refresh()

    def _refresh_run_ids(self) -> None:
        with self._db.connect() as conn:
            new_run_ids = self._db.retrieve_run_ids(conn, self._proposal_id)
            if not new_run_ids and not self._empty_gui_present:
                self._remove_root_items()
                self._empty_gui_present = True
                self._root_layout.addStretch()
                self._root_layout.addWidget(
                    QLabel("No runs present"), 0, Qt.AlignCenter
                )
                refresh_button = _refresh_button(self.style())
                refresh_button.clicked.connect(self._refresh_run_ids)
                refresh_button.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
                new_run_button = QPushButton(
                    self.style().standardIcon(QtWidgets.QStyle.SP_FileDialogNewFolder),
                    "New run",
                )
                new_run_button.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
                new_run_button.clicked.connect(self._slot_manual_new_run)
                self._root_layout.addWidget(refresh_button, 0, Qt.AlignCenter)
                self._root_layout.addWidget(new_run_button, 0, Qt.AlignCenter)
                self._root_layout.addStretch()
            elif new_run_ids:
                if self._empty_gui_present:
                    self._remove_root_items()
                    self._empty_gui_present = False
                self._inner = RunDetailsInner(
                    tables=self._db.tables,
                    run_ids=new_run_ids,
                    samples=self._db.retrieve_mini_samples(conn, self._proposal_id),
                    run=self._db.retrieve_run(conn, max(new_run_ids)),
                    karabo=self._db.retrieve_karabo(conn, max(new_run_ids)),
                    runs_metadata=self._build_runs_metadata(conn),
                )
                self._inner.current_run_changed.connect(self._slot_current_run_changed)
                self._inner.comment_add.connect(self._slot_add_comment)
                self._inner.comment_delete.connect(self._slot_delete_comment)
                self._inner.comment_changed.connect(self._slot_change_comment)
                self._inner.attributo_change.connect(self._slot_attributo_change)
                self._inner.attributo_manual_remove.connect(
                    self._slot_attributo_manual_remove
                )
                self._inner.refresh.connect(self._slot_refresh)
                self._inner.new_attributo.connect(self.new_attributo.emit)
                self._inner.manual_new_run.connect(self._slot_manual_new_run)
                self._root_layout.addWidget(self._inner)
                self._root_layout.setContentsMargins(0, 0, 0, 0)

    def _build_runs_metadata(self, conn: Connection) -> Dict[AttributoId, DBAttributo]:
        md = self._db.run_attributi(conn)
        result: Dict[AttributoId, DBAttributo] = {
            k: v for k, v in md.items() if k not in self._filter_fields
        }
        result[self._db.tables.attributo_run_sample_id] = DBAttributo(
            self._db.tables.attributo_run_sample_id,
            "Sample ID",
            AssociatedTable.RUN,
            AttributoTypeSample(),
        )
        return result

    def _remove_root_items(self):
        while True:
            removed_item = self._root_layout.takeAt(0)
            if removed_item is None:
                break
            if removed_item.widget() is not None:
                removed_item.widget().deleteLater()

    def _slot_refresh(self) -> None:
        with self._db.connect() as conn:
            self._slot_refresh_run(conn)

    def _slot_delete_comment(self, comment_id: int) -> None:
        with self._db.connect() as conn:
            self._db.delete_comment(
                conn,
                self.selected_run_id(),
                comment_id,
            )
            self._slot_refresh_run(conn)
            self.run_changed.emit()

    def _slot_change_comment(self, comment: DBComment) -> None:
        with self._db.connect() as conn:
            self._db.change_comment(conn, comment)
            self._slot_refresh_run(conn)
            self.run_changed.emit()

    def _slot_add_comment(self, comment: DBComment) -> None:
        with self._db.connect() as conn:
            self._db.add_comment(
                conn,
                self.selected_run_id(),
                comment.author,
                comment.text,
            )
            self._slot_refresh_run(conn)
            self.run_changed.emit()

    def _slot_attributo_manual_remove(self, prop: AttributoId) -> None:
        with self._db.connect() as conn:
            selected_run = self.selected_run_id()
            # None as value deletes
            self._db.update_run_attributo(conn, selected_run, prop, None)
            self._slot_refresh_run(conn)
            self.run_changed.emit()

    def _slot_attributo_change(self, prop: AttributoId, new_value: Any) -> None:
        with self._db.connect() as conn:
            selected_run = self.selected_run_id()
            self._db.update_run_attributo(
                conn,
                selected_run,
                prop,
                new_value,
            )
            self._slot_refresh_run(conn)
            self.run_changed.emit()

    def _slot_current_run_changed(self, new_run_id: int) -> None:
        with self._db.connect() as conn:
            self._slot_refresh_run(
                conn,
                new_run_id=new_run_id,
            )

    def _slot_refresh_run(
        self,
        conn: Connection,
        new_run_id: Optional[int] = None,
    ) -> None:
        selected_run_id = self.selected_run_id()
        selected_karabo = self.selected_karabo()
        new_run_id = cast(
            int, new_run_id if new_run_id is not None else selected_run_id
        )
        ids = self._db.retrieve_run_ids(conn, self._proposal_id)
        try:
            r = self._db.retrieve_run(conn, new_run_id)
            cast(RunDetailsInner, self._inner).run_changed(
                r,
                selected_karabo
                if selected_karabo is not None
                else self._db.retrieve_karabo(conn, new_run_id),
                ids,
                self._db.retrieve_mini_samples(conn, self._proposal_id),
                self._build_runs_metadata(conn),
            )
        except RunNotFound:
            if ids:
                self._slot_refresh_run(conn, ids[0])
            else:
                self._refresh_run_ids()

    def selected_run(self) -> DBRun:
        return cast(RunDetailsInner, self._inner).run

    def selected_karabo(self) -> Optional[Karabo]:
        return cast(RunDetailsInner, self._inner).karabo

    def selected_run_id(self) -> int:
        result = self.selected_run().id
        assert isinstance(result, int)
        return result

    def _slot_manual_new_run(self) -> None:
        with self._db.connect() as conn:
            new_run = new_run_dialog(
                parent=self,
                highest_id=max(
                    self._db.retrieve_run_ids(conn, self._proposal_id), default=None
                ),
                samples=self._db.retrieve_mini_samples(conn, self._proposal_id),
            )
            attributi = RawAttributiMap({})
            # attributi.append_single_to_source(
            #     MANUAL_SOURCE_NAME, AttributoId("status"), "running"
            # )
            if new_run is not None:
                if not self._db.add_run(
                    conn, self._proposal_id, new_run.id, new_run.sample_id, attributi
                ):
                    raise Exception("couldn't create run for some reason")
                if self._inner is None:
                    self._refresh_run_ids()
                else:
                    self._slot_refresh_run(conn, new_run.id)
                self.run_changed.emit()
