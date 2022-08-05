import pytest
import structlog.stdlib

from amarcord.amici.kamzik.kamzik_zmq_client import ingest_kamzik_metadata
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.attributi import attributo_type_to_schema
from amarcord.db.attributo_type import AttributoType
from amarcord.db.attributo_type import AttributoTypeInt
from amarcord.db.attributo_type import AttributoTypeString
from amarcord.db.attributo_value import AttributoValue
from amarcord.db.tables import create_tables_from_metadata
from amarcord.json_schema import coparse_schema_type

logger = structlog.stdlib.get_logger(__name__)


async def _get_db() -> AsyncDB:
    context = AsyncDBContext("sqlite+aiosqlite://")
    db = AsyncDB(context, create_tables_from_metadata(context.metadata))
    await db.migrate()
    return db


@pytest.mark.parametrize(
    "input_type_and_value", [(AttributoTypeInt(), 3), (AttributoTypeString(), "foo")]
)
async def test_process_kamzik_metadata(
    input_type_and_value: tuple[AttributoType, AttributoValue]
) -> None:
    db = await _get_db()

    async with db.begin() as conn:
        input_type, input_value = input_type_and_value

        name = "aname"
        await ingest_kamzik_metadata(
            logger,
            db,
            conn,
            {
                "run_id": 1,
                "attributi-schema": {
                    name: coparse_schema_type(attributo_type_to_schema(input_type))
                },
                "attributi-values": {name: input_value},
            },
        )

        attributi = [
            a
            for a in await db.retrieve_attributi(conn, associated_table=None)
            if a.name == name
        ]

        assert len(attributi) == 1
        assert attributi[0].name == name
        assert attributi[0].description == ""
        assert attributi[0].group
        assert attributi[0].associated_table == AssociatedTable.RUN
        assert attributi[0].attributo_type == input_type
