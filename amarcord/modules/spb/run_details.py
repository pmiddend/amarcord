import logging
from typing import Any, Final, Optional, Tuple, cast

from PyQt5 import QtWidgets
from PyQt5.QtCore import Qt, pyqtSignal
from PyQt5.QtWidgets import QLabel, QPushButton, QSizePolicy, QStyle, QWidget

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributo_id import AttributoId
from amarcord.db.constants import MANUAL_SOURCE_NAME
from amarcord.db.db import Connection, DB, RunNotFound
from amarcord.db.karabo import Karabo
from amarcord.db.attributi import AttributiMap, DBAttributo, DBRunComment
from amarcord.db.proposal_id import ProposalId
from amarcord.db.tables import DBTables
from amarcord.modules.context import Context
from amarcord.modules.spb.new_run_dialog import new_run_dialog
from amarcord.modules.spb.run_details_inner import RunDetailsInner

AUTO_REFRESH_TIMER_MSEC: Final = 5000

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
                    sample_ids=self._db.retrieve_sample_ids(conn),
                    run=self._db.retrieve_run(conn, max(new_run_ids)),
                    karabo=self._db.retrieve_karabo(conn, max(new_run_ids)),
                    runs_metadata=self._db.run_attributi(conn),
                )
                self._inner.current_run_changed.connect(self._slot_current_run_changed)
                self._inner.comment_add.connect(self._slot_add_comment)
                self._inner.comment_delete.connect(self._slot_delete_comment)
                self._inner.comment_changed.connect(self._slot_change_comment)
                self._inner.property_change.connect(self._slot_attributo_change)
                self._inner.refresh.connect(self._slot_refresh)
                self._inner.new_attributo.connect(self.new_attributo.emit)
                self._inner.manual_new_run.connect(self._slot_manual_new_run)
                self._root_layout.addWidget(self._inner)

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

    def _slot_change_comment(self, comment: DBRunComment) -> None:
        with self._db.connect() as conn:
            self._db.change_comment(conn, comment)
            self._slot_refresh_run(conn)
            self.run_changed.emit()

    def _slot_add_comment(self, comment: DBRunComment) -> None:
        with self._db.connect() as conn:
            self._db.add_comment(
                conn,
                self.selected_run_id(),
                comment.author,
                comment.text,
            )
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

    def _slot_new_attributo(self, new_column: DBAttributo) -> None:
        with self._db.connect() as conn:
            self._db.add_attributo(
                conn,
                name=new_column.name,
                description=new_column.description,
                suffix=None,
                prop_type=new_column.rich_property_type,
                associated_table=AssociatedTable.RUN,
            )
            cast(RunDetailsInner, self._inner).runs_metadata_changed(
                self._db.run_attributi(conn)
            )
            self.run_changed.emit()

    def _slot_current_run_changed(self, new_run_id: int) -> None:
        with self._db.connect() as conn:
            self._slot_refresh_run(
                conn,
                new_run_id=new_run_id,
            )

    def _retrieve_run_with_karabo(
        self, conn: Connection, run_id: int, old_karabo: Optional[Karabo]
    ) -> Tuple[AttributiMap, Optional[Karabo]]:
        return self._db.retrieve_run(conn, run_id), (
            self._db.retrieve_karabo(conn, run_id) if old_karabo is None else old_karabo
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
                self._db.retrieve_sample_ids(conn),
                self._db.run_attributi(conn),
            )
        except RunNotFound:
            if ids:
                self._slot_refresh_run(conn, ids[0])
            else:
                self._refresh_run_ids()

    def selected_run(self) -> AttributiMap:
        return cast(RunDetailsInner, self._inner).run

    def selected_karabo(self) -> Optional[Karabo]:
        return cast(RunDetailsInner, self._inner).karabo

    def selected_run_id(self) -> int:
        result = self.selected_run().select_int_unsafe(self._db.tables.attributo_run_id)
        assert isinstance(result, int)
        return result

    def _slot_manual_new_run(self) -> None:
        with self._db.connect() as conn:
            new_run = new_run_dialog(
                parent=self,
                highest_id=max(
                    self._db.retrieve_run_ids(conn, self._proposal_id), default=None
                ),
                sample_ids=self._db.retrieve_sample_ids(conn),
            )
            attributi = AttributiMap({})
            attributi.append_single_to_source(
                MANUAL_SOURCE_NAME, AttributoId("status"), "running"
            )
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
