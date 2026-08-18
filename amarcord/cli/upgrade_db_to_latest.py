import asyncio

import structlog
from sqlalchemy.ext.asyncio import create_async_engine
from typed_argparse import Parser
from typed_argparse import TypedArgs
from typed_argparse import arg

from amarcord.db.orm_utils import migrate
from amarcord.logging_util import setup_structlog

setup_structlog()

logger = structlog.stdlib.get_logger(__name__)


class Arguments(TypedArgs):
    db_connection_url: str = arg(
        help="Connection URL for the database to export from (e.g. mysql+pymysql://foo/bar"
    )


async def _upgrade_db_to_latest(args: Arguments) -> None:
    engine = create_async_engine(args.db_connection_url)
    await migrate(engine)
    # Important - aiosqlite will hang if this is omitted.
    await engine.dispose()
    logger.info(
        f"database at {args.db_connection_url} updated to latest version, it's now ready to use!",
    )


def main() -> None:
    def run(args: Arguments) -> None:
        asyncio.run(_upgrade_db_to_latest(args))

    Parser(Arguments).bind(run).run()


if __name__ == "__main__":
    main()
