import datetime
import logging
from time import sleep
from typing import Optional

from PyQt5.QtCore import QObject
from PyQt5.QtCore import QThread

from amarcord.db.db import DB
from amarcord.db.table_classes import DBEvent
from amarcord.db.tables import DBTables
from amarcord.modules.dbcontext import DBContext

logger = logging.getLogger(__name__)


def _log(e: DBEvent) -> None:
    logger.log(
        e.level.to_python_log_level(),
        f"source={e.source}: {e.text}",
    )


class Worker(QObject):
    def __init__(self, db: DB, last_id: Optional[int]) -> None:
        super().__init__()
        self._db = db
        self._last_id = last_id

    def run(self) -> None:
        while True:
            sleep(3)
            with self._db.connect() as conn:
                for e in self._db.retrieve_events(conn, self._last_id):
                    _log(e)


class EventLogDaemon(QObject):
    def __init__(self, db: DBContext, tables: DBTables, parent: QObject) -> None:
        super().__init__(parent)
        self._db = DB(db, tables)
        self._last_id: Optional[int] = None

        with self._db.connect() as conn:
            now = datetime.datetime.utcnow()
            for e in self._db.retrieve_events(
                conn, since=now - datetime.timedelta(minutes=10)
            ):
                _log(e)
                self._last_id = e.id

        self._thread = QThread()
        self._worker = Worker(self._db, self._last_id)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._thread.start()
