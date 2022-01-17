import pytest_asyncio

from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.tables import create_tables_from_metadata
from amarcord.db.dbcontext import CreationMode


@pytest_asyncio.fixture
async def db() -> AsyncDB:
    context = AsyncDBContext("sqlite+aiosqlite://")
    # pylint: disable=assigning-non-slot
    result = AsyncDB(context, create_tables_from_metadata(context.metadata))
    await context.create_all(CreationMode.CHECK_FIRST)
    return result
