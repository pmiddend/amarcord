from typing import Tuple

import pytest

from amarcord.amici.kamzik.kamzik_client import process_kamzik_metadata
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.attributi import attributo_type_to_schema
from amarcord.db.attributo_type import (
    AttributoTypeInt,
    AttributoType,
    AttributoTypeString,
)
from amarcord.db.attributo_value import AttributoValue
from amarcord.db.dbcontext import CreationMode
from amarcord.db.tables import create_tables_from_metadata


async def _get_db() -> AsyncDB:
    context = AsyncDBContext("sqlite+aiosqlite://")
    db = AsyncDB(context, create_tables_from_metadata(context.metadata))
    await context.create_all(CreationMode.DONT_CHECK)
    return db


@pytest.mark.parametrize(
    "input_type_and_value", [(AttributoTypeInt(), 3), (AttributoTypeString(), "foo")]
)
async def test_process_kamzik_metadata(
    input_type_and_value: Tuple[AttributoType, AttributoValue]
) -> None:
    db = await _get_db()

    async with db.begin() as conn:
        input_type, input_value = input_type_and_value

        name = "aname"
        await process_kamzik_metadata(
            db,
            conn,
            1,
            {name: attributo_type_to_schema(input_type)},
            {name: input_value},
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        assert len(attributi) == 1
        assert attributi[0].name == name
        assert attributi[0].description == ""
        assert attributi[0].group
        assert attributi[0].associated_table == AssociatedTable.RUN
        assert attributi[0].attributo_type == input_type
