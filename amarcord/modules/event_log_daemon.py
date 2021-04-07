import datetime
import logging
from typing import Optional

from PyQt5.QtCore import QObject
from PyQt5.QtCore import QTimer

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


class EventLogDaemon(QTimer):
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

        self.timeout.connect(self._update)
        self.start(2000)

    def _update(self):
        with self._db.connect() as conn:
            for e in self._db.retrieve_events(conn, self._last_id):
                _log(e)
                self._last_id = e.id
