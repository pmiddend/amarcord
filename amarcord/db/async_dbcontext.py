from sqlalchemy import MetaData
from sqlalchemy import event
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import StaticPool

from amarcord.db.dbcontext import Connection
from amarcord.db.dbcontext import CreationMode


class AsyncDBContext:
    def __init__(self, connection_url: str, echo: bool = False) -> None:
        # For the sqlite in-memory stuff, see here:
        #
        # https://docs.sqlalchemy.org/en/13/dialects/sqlite.html#threading-pooling-behavior
        #
        # In short, with in-memory databases, we normally get one DB per thread, which is bad if we have
        # background threads updating the DB (in test scenarios, of course).
        in_memory_db = connection_url == "sqlite://"
        self.engine = create_async_engine(
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

    async def create_all(self, creation_mode: CreationMode) -> None:
        async with self.connect() as conn:
            await conn.run_sync(
                lambda myengine: self.metadata.create_all(
                    myengine, checkfirst=creation_mode == CreationMode.CHECK_FIRST
                )
            )

    def connect(self) -> Connection:
        return self.engine.connect()

    async def dispose(self) -> None:
        await self.engine.dispose()
