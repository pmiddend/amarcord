from enum import Enum
from enum import auto
from typing import Any
from typing import Callable
from typing import List

from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy import event


class CreationMode(Enum):
    CHECK_FIRST = auto()
    DONT_CHECK = auto()


class DBContext:
    def __init__(self, connection_url: str) -> None:
        self.engine = create_engine(connection_url, echo=False)

        # sqlite doesn't care about foreign keys unless you do this dance, see
        # https://stackoverflow.com/questions/2614984/sqlite-sqlalchemy-how-to-enforce-foreign-keys
        def _fk_pragma_on_connect(dbapi_con, _con_record):
            dbapi_con.execute("pragma foreign_keys=ON")

        if "sqlite" in connection_url:
            event.listen(self.engine, "connect", _fk_pragma_on_connect)

        self.metadata = MetaData()
        self._after_db_created: List[Callable[[], None]] = []
        self._db_created = False

    def after_db_created(self, f: Callable[[], None]) -> None:
        if self._db_created:
            f()
        else:
            self._after_db_created.append(f)

    @property
    def dbname(self) -> str:
        return self.engine.url.database

    def create_all(self, creation_mode: CreationMode) -> None:
        self.metadata.create_all(
            self.engine,
            checkfirst=creation_mode == CreationMode.CHECK_FIRST,
        )
        self._db_created = True
        for f in self._after_db_created:
            f()
        self._after_db_created = []

    def connect(self) -> Any:
        return self.engine.connect()
