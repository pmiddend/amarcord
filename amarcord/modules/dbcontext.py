from typing import Callable
from typing import List
from typing import Any
from enum import Enum
from enum import auto
from sqlalchemy import (
    create_engine,
    MetaData,
)


class CreationMode(Enum):
    CHECK_FIRST = auto()
    DONT_CHECK = auto()


class DBContext:
    def __init__(self, connection_url: str) -> None:
        self.engine = create_engine(connection_url, echo=True)
        self.metadata = MetaData()
        self._after_db_created: List[Callable[[], None]] = []

    def after_db_created(self, f: Callable[[], None]) -> None:
        self._after_db_created.append(f)

    @property
    def dbname(self) -> str:
        return self.engine.url.database

    def create_all(self, creation_mode: CreationMode) -> None:
        self.metadata.create_all(
            self.engine, checkfirst=creation_mode == CreationMode.CHECK_FIRST
        )
        for f in self._after_db_created:
            f()
        self._after_db_created = []

    def connect(self) -> Any:
        return self.engine.connect()
