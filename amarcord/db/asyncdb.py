import re
from typing import List

import sqlalchemy as sa

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.attributi import attributo_type_to_schema
from amarcord.db.attributi import schema_json_to_attributo_type
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoType
from amarcord.db.constants import ATTRIBUTO_NAME_REGEX
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.dbcontext import Connection
from amarcord.db.tables import DBTables


class AsyncDB:
    def __init__(self, dbcontext: AsyncDBContext, tables: DBTables) -> None:
        self.dbcontext = dbcontext
        self.tables = tables

    def connect(self) -> Connection:
        return self.dbcontext.connect()

    def begin(self) -> Connection:
        return self.dbcontext.begin()

    async def dispose(self) -> None:
        await self.dbcontext.dispose()

    async def retrieve_attributi(
        self,
        conn: Connection,
    ) -> List[DBAttributo]:
        select_stmt = sa.select(
            [
                self.tables.attributo.c.name,
                self.tables.attributo.c.description,
                self.tables.attributo.c.json_schema,
                self.tables.attributo.c.associated_table,
            ]
        ).order_by(self.tables.attributo.c.associated_table)

        result = await conn.execute(select_stmt)
        return [
            DBAttributo(
                name=AttributoId(a["name"]),
                description=a["description"],
                associated_table=a["associated_table"],
                attributo_type=schema_json_to_attributo_type(a["json_schema"]),
            )
            for a in result
        ]

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

    async def delete_attributo(
        self,
        conn: Connection,
        name: str,
    ) -> None:
        await conn.execute(
            sa.delete(self.tables.attributo).where(self.tables.attributo.c.name == name)
        )

    async def change_attributo(
        self, conn: Connection, name: str, new_attributo: DBAttributo
    ) -> None:
        current_attributi = await self.retrieve_attributi(conn)

        current_attributo = next((x for x in current_attributi if x.name == name), None)

        if current_attributo is None:
            raise Exception(
                f"couldn't find attributo for table {new_attributo.associated_table} and name {name}"
            )

        existing_attributo = next(
            (a for a in current_attributi if a.name == new_attributo.name), None
        )
        if new_attributo.name != name and existing_attributo is not None:
            raise Exception(
                f"cannot rename {name} to {new_attributo.name} because we already have an attributo of that "
                "name"
            )

        # first, change the attributo itself, then its actual values (if possible)
        # TODO: add a transaction if runs/samples are also changed
        await conn.execute(
            sa.update(self.tables.attributo)
            .values(
                name=new_attributo.name,
                description=new_attributo.description,
                json_schema=attributo_type_to_schema(new_attributo.attributo_type),
            )
            .where(self.tables.attributo.c.name == name)
        )

        # TODO
        #
        # if new_attributo.associated_table == AssociatedTable.RUN:
        #     for run in self.retrieve_runs(conn, proposal_id=None, since=None):
        #         changed = AttributiMap(
        #             current_attributi[new_attributo.associated_table], run.attributi
        #         ).convert_attributo(AttributoId(name), new_attributo.attributo_type)
        #         if changed:
        #             self.update_run_attributi(
        #                 conn,
        #                 run.id,
        #                 run.attributi,
        #             )
