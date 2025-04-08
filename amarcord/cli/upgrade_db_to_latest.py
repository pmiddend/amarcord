import asyncio

import structlog
from sqlalchemy.ext.asyncio import create_async_engine
from tap import Tap

from amarcord.db.orm_utils import migrate
from amarcord.logging_util import setup_structlog

setup_structlog()

logger = structlog.stdlib.get_logger(__name__)


class Arguments(Tap):
    db_connection_url: (
        str  # Connection URL for the database (e.g. pymysql+mysql://foo/bar)
    )


async def _upgrade_db_to_latest(args: Arguments) -> None:
    engine = create_async_engine(args.db_connection_url)
    await migrate(engine)
    logger.info(
        f"database at {args.db_connection_url} updated to latest version, it's now ready to use!",
    )


def main() -> None:
    asyncio.run(
        _upgrade_db_to_latest(Arguments(underscores_to_dashes=True).parse_args()),
    )


if __name__ == "__main__":
    main()
