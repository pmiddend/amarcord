import datetime
import itertools
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, cast, Tuple, Dict
from typing import Optional

import magic
import sqlalchemy as sa

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.attributi import AttributoConversionFlags
from amarcord.db.attributi import attributo_type_to_schema
from amarcord.db.attributi import schema_json_to_attributo_type
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoType
from amarcord.db.constants import ATTRIBUTO_NAME_REGEX
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.dbcontext import Connection
from amarcord.db.table_classes import DBSample, DBFile
from amarcord.db.tables import DBTables
from amarcord.util import sha256_file


@dataclass(frozen=True)
class CreateFileResult:
    id: int
    type_: str


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

    async def retrieve_file(
        self, conn: Connection, file_id: int
    ) -> Tuple[str, str, bytes]:
        result = await conn.execute(
            sa.select(
                [
                    self.tables.file.c.file_name,
                    self.tables.file.c.type,
                    self.tables.file.c.contents,
                ]
            ).where(self.tables.file.c.id == file_id)
        )

        return result.fetchone()

    async def retrieve_attributi(
        self, conn: Connection, associated_table: Optional[AssociatedTable]
    ) -> List[DBAttributo]:
        select_stmt = sa.select(
            [
                self.tables.attributo.c.name,
                self.tables.attributo.c.description,
                self.tables.attributo.c.json_schema,
                self.tables.attributo.c.associated_table,
            ]
        ).order_by(self.tables.attributo.c.associated_table)

        if associated_table is not None:
            select_stmt = select_stmt.where(
                self.tables.attributo.c.associated_table == associated_table
            )

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

    async def create_sample(
        self,
        conn: Connection,
        name: str,
        attributi: AttributiMap,
    ) -> int:
        return (
            await conn.execute(
                self.tables.sample.insert().values(
                    name=name,
                    modified=datetime.datetime.utcnow(),
                    attributi=attributi.to_raw(),
                )
            )
        ).inserted_primary_key[0]

    async def update_sample(
        self,
        conn: Connection,
        id_: int,
        name: str,
        attributi: AttributiMap,
    ) -> None:
        await conn.execute(
            sa.update(self.tables.sample)
            .values(
                name=name,
                modified=datetime.datetime.utcnow(),
                attributi=attributi.to_raw(),
            )
            .where(self.tables.sample.c.id == id_)
        )

    async def retrieve_samples(
        self, conn: Connection, attributi: List[DBAttributo]
    ) -> List[DBSample]:
        file_results = (
            await conn.execute(
                sa.select(
                    [
                        self.tables.sample_has_file.c.sample_id,
                        self.tables.file.c.id,
                        self.tables.file.c.description,
                        self.tables.file.c.file_name,
                        self.tables.file.c.type,
                    ]
                )
                .select_from(
                    self.tables.sample_has_file.join(
                        self.tables.file,
                        self.tables.sample_has_file.c.file_id == self.tables.file.c.id,
                    )
                )
                .order_by(self.tables.sample_has_file.c.sample_id)
            )
        ).fetchall()

        sample_to_files: Dict[int, List[DBFile]] = {}

        for key, group in itertools.groupby(
            file_results, key=lambda row: row["sample_id"]
        ):
            sample_to_files[key] = [
                DBFile(
                    id=row["id"],
                    description=row["description"],
                    type_=row["type"],
                    file_name=row["file_name"],
                )
                for row in group
            ]

        select_stmt = sa.select(
            [
                self.tables.sample.c.id,
                self.tables.sample.c.name,
                self.tables.sample.c.attributi,
            ]
        ).order_by(self.tables.sample.c.name)

        result = await conn.execute(select_stmt)

        return [
            DBSample(
                id=a["id"],
                name=a["name"],
                attributi=AttributiMap(
                    types=attributi,
                    raw_attributi=a["attributi"],
                ),
                files=sample_to_files.get(a["id"], []),
            )
            for a in result
        ]

    async def delete_file(
        self,
        conn: Connection,
        id_: int,
    ) -> None:
        await conn.execute(
            sa.delete(self.tables.file).where(self.tables.file.c.id == id_)
        )

    async def delete_sample(
        self,
        conn: Connection,
        id_: int,
    ) -> None:
        await conn.execute(
            sa.delete(self.tables.sample).where(self.tables.sample.c.id == id_)
        )

    async def create_attributo(
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
        attributi = await self.retrieve_attributi(conn, associated_table=None)

        found_attributo = next((x for x in attributi if x.name == name), None)
        if found_attributo is None:
            raise Exception(f'couldn\'t find attributo "{name}"')

        await conn.execute(
            sa.delete(self.tables.attributo).where(self.tables.attributo.c.name == name)
        )

        if found_attributo.associated_table == AssociatedTable.SAMPLE:
            # This is the tricky bit: we need to retrieve the samples with the old attributi list. The samples haven't
            # been converted to the new format, so using the new attributi list would make that fail validation.
            for s in await self.retrieve_samples(conn, attributi):
                # Then remove the attributo from the sample and the accompanying types, and update.
                s.attributi.remove(name)
                await self.update_sample(conn, cast(int, s.id), s.name, s.attributi)
        else:
            # FIXME: do this for runs
            pass

    async def create_file(
        self,
        conn: Connection,
        file_name: str,
        description: str,
        contents_location: Path,
    ) -> CreateFileResult:
        mime = magic.from_file(str(contents_location), mime=True)

        sha256 = sha256_file(contents_location)
        with contents_location.open("rb") as f:
            return CreateFileResult(
                id=(
                    await conn.execute(
                        self.tables.file.insert().values(
                            type=mime,
                            modified=datetime.datetime.utcnow(),
                            # FIXME: Don't load the whole thing into memory ffs
                            contents=f.read(),
                            file_name=file_name,
                            description=description,
                            sha256=sha256,
                        )
                    )
                ).inserted_primary_key[0],
                type_=mime,
            )

    async def update_attributo(
        self,
        conn: Connection,
        name: str,
        # pylint: disable=unused-argument
        conversion_flags: AttributoConversionFlags,
        new_attributo: DBAttributo,
    ) -> None:
        current_attributi = await self.retrieve_attributi(conn, associated_table=None)

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
        await conn.execute(
            sa.update(self.tables.attributo)
            .values(
                name=new_attributo.name,
                description=new_attributo.description,
                json_schema=attributo_type_to_schema(new_attributo.attributo_type),
            )
            .where(self.tables.attributo.c.name == name)
        )

        if new_attributo.associated_table == AssociatedTable.SAMPLE:
            for s in await self.retrieve_samples(conn, current_attributi):
                s.attributi.convert_attributo(
                    conversion_flags, name, new_attributo.attributo_type
                )
                await self.update_sample(conn, cast(int, s.id), s.name, s.attributi)

    async def add_file_to_sample(
        self, conn: Connection, file_id: int, sample_id: int
    ) -> None:
        await conn.execute(
            sa.insert(self.tables.sample_has_file).values(
                file_id=file_id, sample_id=sample_id
            )
        )

    async def remove_files_from_sample(self, conn: Connection, sample_id: int) -> None:
        await conn.execute(
            sa.delete(self.tables.sample_has_file).where(
                self.tables.sample_has_file.c.sample_id == sample_id
            )
        )
