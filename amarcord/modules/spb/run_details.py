import logging
from dataclasses import replace
from typing import Any, Final, Optional, cast

from PyQt5 import QtWidgets
from PyQt5.QtCore import QVariant, Qt, pyqtSignal
from PyQt5.QtWidgets import QLabel, QPushButton, QSizePolicy, QStyle, QWidget

from amarcord.modules.context import Context
from amarcord.modules.spb.db import (
    Connection,
    CustomRunProperty,
    DB,
    DBRun,
    DBRunComment,
)
from amarcord.modules.spb.db_tables import DBTables
from amarcord.modules.spb.new_run_dialog import new_run_dialog
from amarcord.modules.spb.proposal_id import ProposalId
from amarcord.modules.spb.run_details_inner import RunDetailsInner
from amarcord.modules.spb.run_property import RunProperty

AUTO_REFRESH_TIMER_MSEC: Final = 5000

logger = logging.getLogger(__name__)


def _refresh_button(style: QStyle) -> QPushButton:
    return QPushButton(
        style.standardIcon(QtWidgets.QStyle.SP_BrowserReload),
        "Refresh",
    )


class RunDetails(QWidget):
    run_changed = pyqtSignal()

    def __init__(
        self, context: Context, tables: DBTables, proposal_id: ProposalId
    ) -> None:
        super().__init__()

        self._proposal_id = proposal_id
        self._context = context
        self._db = DB(context.db, tables)

        self._root_layout = QtWidgets.QVBoxLayout()
        self.setLayout(self._root_layout)
        self._inner: Optional[RunDetailsInner] = None
        self._first_run = True

        self._refresh_run_ids()

    def _refresh_run_ids(self) -> None:
        with self._db.connect() as conn:
            new_run_ids = self._db.retrieve_run_ids(conn, self._proposal_id)
            if not new_run_ids and self._first_run:
                self._first_run = False
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
                if not self._first_run:
                    while True:
                        removed_item = self._root_layout.takeAt(0)
                        if removed_item is None:
                            break
                        if removed_item.widget() is not None:
                            removed_item.widget().deleteLater()
                self._inner = RunDetailsInner(
                    tables=self._db.tables,
                    run_ids=new_run_ids,
                    sample_ids=self._db.retrieve_sample_ids(conn),
                    run=self._db.retrieve_run(conn, max(new_run_ids)),
                    runs_metadata=self._db.run_property_metadata(conn),
                )
                self._inner.current_run_changed.connect(self._slot_current_run_changed)
                self._inner.comment_add.connect(self._slot_add_comment)
                self._inner.comment_delete.connect(self._slot_delete_comment)
                self._inner.comment_changed.connect(self._slot_change_comment)
                self._inner.property_change.connect(self._slot_property_change)
                self._inner.refresh.connect(self._slot_refresh)
                self._inner.new_custom_column.connect(self._slot_new_custom_column)
                self._inner.manual_new_run.connect(self._slot_manual_new_run)
                self._root_layout.addWidget(self._inner)

    def _slot_refresh(self) -> None:
        with self._db.connect() as conn:
            self._slot_refresh_run(conn, self.selected_run())

    def _slot_delete_comment(self, comment_id: int) -> None:
        with self._db.connect() as conn:
            self._db.delete_comment(
                conn,
                self.selected_run_id(),
                comment_id,
            )
            self._slot_refresh_run(conn, self.selected_run())
            self.run_changed.emit()

    def _slot_change_comment(self, comment: DBRunComment) -> None:
        with self._db.connect() as conn:
            self._db.change_comment(conn, comment)
            self._slot_refresh_run(conn, self.selected_run())
            self.run_changed.emit()

    def _slot_add_comment(self, comment: DBRunComment) -> None:
        with self._db.connect() as conn:
            self._db.add_comment(
                conn,
                self.selected_run_id(),
                comment.author,
                comment.text,
            )
            self._slot_refresh_run(conn, self.selected_run())
            self.run_changed.emit()

    def _slot_property_change(self, prop: RunProperty, new_value: Any) -> None:
        with self._db.connect() as conn:
            selected_run = self.selected_run_id()
            self._db.update_run_property(
                conn,
                selected_run,
                prop,
                new_value,
            )
            self._slot_refresh_run(conn, self.selected_run())
            self.run_changed.emit()

    def _slot_new_custom_column(self, new_column: CustomRunProperty) -> None:
        with self._db.connect() as conn:
            self._db.add_custom_run_property(
                conn,
                name=new_column.name,
                description=new_column.description,
                suffix=None,
                prop_type=new_column.rich_property_type,
            )
            cast(RunDetailsInner, self._inner).runs_metadata_changed(
                self._db.run_property_metadata(conn)
            )
            self.run_changed.emit()

    def _slot_current_run_changed(self, new_run_id: int) -> None:
        with self._db.connect() as conn:
            self._slot_refresh_run(
                conn,
                old_run=self.selected_run(),
                new_run_id=new_run_id,
            )

    def _run_selector_changed(self, conn: Connection) -> None:
        new_run_ids = self._db.retrieve_run_ids(conn, self._proposal_id)

        if new_run_ids != self._run_ids:
            self._run_ids = new_run_ids
            self._run_selector.blockSignals(True)
            self._run_selector.clear()
            self._run_selector.addItems([str(r) for r in self._run_ids])

        if self._run is not None:
            self._run_selector.setCurrentText(
                str(self._run.properties[self._db.tables.property_run_id])
            )
        self._run_selector.blockSignals(False)

    def _slot_refresh_run(
        self,
        conn: Connection,
        old_run: Optional[DBRun] = None,
        new_run_id: Optional[int] = None,
    ) -> DBRun:
        selected_run_id = self.selected_run_id()
        old_modified = (
            old_run.properties[self._db.tables.property_modified]
            if old_run is not None
            else None
        )
        old_karabo = old_run.karabo if old_run is not None else None
        new_run_id = cast(
            int, new_run_id if new_run_id is not None else selected_run_id
        )
        new_run = self._db.retrieve_run(conn, new_run_id)

        if (
            new_run.properties[self._db.tables.property_modified] == old_modified
            and selected_run_id == new_run_id
        ):
            logger.info("no updates, not refreshing")
            return new_run

        new_run = replace(
            new_run,
            karabo=self._db.retrieve_karabo(conn, new_run_id)
            if old_karabo is None
            else old_karabo,
        )

        cast(RunDetailsInner, self._inner).run_changed(
            new_run,
            self._db.retrieve_run_ids(conn, self._proposal_id),
            self._db.retrieve_sample_ids(conn),
            self._db.run_property_metadata(conn),
        )

        return new_run

    def selected_run(self) -> DBRun:
        return cast(RunDetailsInner, self._inner).run

    def selected_run_id(self) -> int:
        return self.selected_run().properties[self._db.tables.property_run_id]

    def _slot_manual_new_run(self) -> None:
        with self._db.connect() as conn:
            new_run = new_run_dialog(
                parent=self,
                highest_id=max(
                    self._db.retrieve_run_ids(conn, self._proposal_id), default=None
                ),
                sample_ids=self._db.retrieve_sample_ids(conn),
            )
            if new_run is not None:
                if not self._db.create_run(
                    conn, self._proposal_id, new_run.id, new_run.sample_id
                ):
                    raise Exception("couldn't create run for some reason")
                if self._inner is None:
                    self._refresh_run_ids()
                else:
                    self._slot_refresh_run(conn, self.selected_run(), new_run.id)
                self.run_changed.emit()
