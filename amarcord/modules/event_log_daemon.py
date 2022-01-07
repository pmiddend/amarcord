import datetime
import logging
from time import sleep
from typing import Optional

from PyQt5.QtCore import QObject
from PyQt5.QtCore import QThread
from PyQt5.QtCore import QVariant
from PyQt5.QtCore import pyqtSignal
from PyQt5.QtWidgets import QPlainTextEdit

from amarcord.db.db import DB
from amarcord.db.dbcontext import DBContext
from amarcord.db.table_classes import DBEvent
from amarcord.db.tables import DBTables

logger = logging.getLogger(__name__)


def _log(e: DBEvent) -> None:
    logger.log(
        e.level.to_python_log_level(),
        f"source={e.source}: {e.text}",
    )


class Worker(QObject):
    new_event = pyqtSignal(QVariant)

    def __init__(self, db: DB, last_id: Optional[int]) -> None:
        super().__init__()
        self._db = db
        self._last_id = last_id

    def run(self) -> None:
        while True:
            sleep(3)
            with self._db.connect() as conn:
                for e in self._db.retrieve_events(conn, self._last_id):
                    self.new_event.emit(e)
                    self._last_id = e.id


class EventLogDaemon(QObject):
    def __init__(
        self,
        log_output: QPlainTextEdit,
        db: DBContext,
        tables: DBTables,
        parent: QObject,
    ) -> None:
        super().__init__(parent)
        self._db = DB(db, tables)
        self._last_id: Optional[int] = None
        self._log_output = log_output

        with self._db.connect() as conn:
            now = datetime.datetime.utcnow()
            for e in self._db.retrieve_events(
                conn, since=now - datetime.timedelta(minutes=10)
            ):
                self._output_event(e)
                self._last_id = e.id

        self._thread = QThread()
        self._worker = Worker(self._db, self._last_id)
        self._worker.moveToThread(self._thread)
        self._worker.new_event.connect(self._output_event)
        self._thread.started.connect(self._worker.run)  # type: ignore
        self._thread.start()

    def _output_event(self, e: DBEvent) -> None:
        time = (
            e.created.replace(tzinfo=datetime.timezone.utc)
            .astimezone(tz=None)
            .strftime("%Y-%m-%dT%H:%M:%S")
        )
        self._log_output.appendPlainText(f"{time} {e.level.name}: {e.source}: {e.text}")
