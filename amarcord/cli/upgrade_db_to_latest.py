import asyncio

from tap import Tap

from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.tables import create_tables_from_metadata


class Arguments(Tap):
    db_connection_url: (
        str  # Connection URL for the database (e.g. pymysql+mysql://foo/bar)
    )


async def _upgrade_db_to_latest(args: Arguments) -> None:
    db_context = AsyncDBContext(args.db_connection_url)
    db = AsyncDB(db_context, create_tables_from_metadata(db_context.metadata))
    await db.migrate()
    print(
        f"database at {args.db_connection_url} updated to latest version, it's now ready to use!"
    )


def main() -> None:
    asyncio.run(
        _upgrade_db_to_latest(Arguments(underscores_to_dashes=True).parse_args())
    )


if __name__ == "__main__":
    main()
