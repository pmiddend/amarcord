import json
from enum import Enum
from enum import auto
from typing import Any

from sqlalchemy import MetaData
from sqlalchemy import event
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.pool import StaticPool

Connection = Any


class CreationMode(Enum):
    CHECK_FIRST = auto()
    DONT_CHECK = auto()


def _json_serializer_allow_nan_false(obj: Any, **kwargs: Any) -> str:
    return json.dumps(obj, **kwargs, allow_nan=False)


class AsyncDBContext:
    def __init__(
        self,
        connection_url: str,
        echo: bool = False,
        use_sqlalchemy_default_json_serializer: bool = False,
    ) -> None:
        # For the sqlite in-memory stuff, see here:
        #
        # https://docs.sqlalchemy.org/en/13/dialects/sqlite.html#threading-pooling-behavior
        #
        # In short, with in-memory databases, we normally get one DB per thread, which is bad if we have
        # background threads updating the DB (in test scenarios, of course).
        in_memory_db = connection_url == "sqlite+aiosqlite://"
        self.engine = create_async_engine(
            connection_url,
            echo=echo,
            connect_args={"check_same_thread": False} if in_memory_db else {},
            poolclass=StaticPool if in_memory_db else NullPool,
            json_serializer=_json_serializer_allow_nan_false
            if not use_sqlalchemy_default_json_serializer
            else None,
            execution_options={"isolation_level": "SERIALIZABLE"},
        )

        # sqlite doesn't care about foreign keys unless you do this dance, see
        # https://stackoverflow.com/questions/2614984/sqlite-sqlalchemy-how-to-enforce-foreign-keys
        def _fk_pragma_on_connect(dbapi_con: Any, _con_record: Any) -> None:
            dbapi_con.execute("pragma foreign_keys=ON")

        if "sqlite" in connection_url:
            event.listen(self.engine.sync_engine, "connect", _fk_pragma_on_connect)

        self.metadata = MetaData()

    async def create_all(self, creation_mode: CreationMode) -> None:
        async with self.read_only_connection() as conn:
            # Not sure what to do in order to convince pyright we actually know the lambda type
            await conn.run_sync(
                lambda myengine: self.metadata.create_all(  # pyright: ignore[reportUnknownLambdaType]
                    myengine,  # pyright: ignore [reportUnknownArgumentType]
                    checkfirst=creation_mode == CreationMode.CHECK_FIRST,
                )
            )

    def begin(self) -> Connection:
        return self.engine.begin()

    def read_only_connection(self) -> Connection:
        return self.engine.connect()

    async def dispose(self) -> None:
        await self.engine.dispose()
