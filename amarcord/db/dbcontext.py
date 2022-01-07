from typing import Any
from typing import Callable
from typing import Dict
from typing import List

from enum import Enum
from enum import auto
from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy.pool import StaticPool


class CreationMode(Enum):
    CHECK_FIRST = auto()
    DONT_CHECK = auto()


Connection = Any


class DBContext:
    def __init__(self, connection_url: str, echo: bool = False) -> None:
        # For the sqlite in-memory stuff, see here:
        #
        # https://docs.sqlalchemy.org/en/13/dialects/sqlite.html#threading-pooling-behavior
        #
        # In short, with in-memory databases, we normally get one DB per thread, which is bad if we have
        # background threads updating the DB (in test scenarios, of course).
        in_memory_db = connection_url == "sqlite://"
        self.engine = create_engine(
            connection_url,
            echo=echo,
            connect_args={"check_same_thread": False} if in_memory_db else {},
            poolclass=StaticPool if in_memory_db else None,
        )

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

    def create_all(self, creation_mode: CreationMode) -> None:
        self.metadata.create_all(
            self.engine,
            checkfirst=creation_mode == CreationMode.CHECK_FIRST,
        )
        self._db_created = True
        for f in self._after_db_created:
            f()
        self._after_db_created = []

    def dump_schema(self) -> None:
        v: Dict[int, Any] = {}

        def _metadata_dump(sql):
            print(sql.compile(dialect=v[0].dialect))

        engine = create_engine(
            "sqlite://", echo=False, strategy="mock", executor=_metadata_dump
        )
        v[0] = engine
        self.metadata.create_all(engine)

    def connect(self) -> Connection:
        return self.engine.connect()
