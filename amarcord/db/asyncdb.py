import re

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.attributi import attributo_type_to_schema
from amarcord.db.attributo_type import AttributoType
from amarcord.db.constants import ATTRIBUTO_NAME_REGEX
from amarcord.db.dbcontext import Connection
from amarcord.db.tables import DBTables


class AsyncDB:
    def __init__(self, dbcontext: AsyncDBContext, tables: DBTables) -> None:
        self.dbcontext = dbcontext
        self.tables = tables

    async def add_attributo(
        self,
        conn: Connection,
        name: str,
        description: str,
        associated_table: AssociatedTable,
        type_: AttributoType,
    ) -> None:
        if not re.fullmatch(ATTRIBUTO_NAME_REGEX, name, re.IGNORECASE):
            raise ValueError(
                f'attributo name "{name}" contains invalid characters (maybe a number at the beginning '
                f"or a dash?)"
            )
        await conn.execute(
            self.tables.attributo.insert().values(
                name=name,
                description=description,
                associated_table=associated_table,
                json_schema=attributo_type_to_schema(type_),
            )
        )

    def connect(self) -> Connection:
        return self.dbcontext.connect()

    async def dispose(self) -> None:
        await self.dbcontext.dispose()
