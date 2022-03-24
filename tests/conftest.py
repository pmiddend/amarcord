import pytest_asyncio

from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.tables import create_tables_from_metadata


@pytest_asyncio.fixture
async def db() -> AsyncDB:
    context = AsyncDBContext("sqlite+aiosqlite://")
    # pylint: disable=assigning-non-slot
    result = AsyncDB(context, create_tables_from_metadata(context.metadata))
    await result.migrate()
    return result
