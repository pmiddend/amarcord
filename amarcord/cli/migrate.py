import asyncio
import logging
import sys

from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.tables import create_tables_from_metadata

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)


async def main():
    context = AsyncDBContext(connection_url=sys.argv[1])
    # pylint: disable=assigning-non-slot
    instance = AsyncDB(context, create_tables_from_metadata(context.metadata))
    await instance.migrate()


asyncio.run(main())
