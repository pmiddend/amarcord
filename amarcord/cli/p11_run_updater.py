import asyncio
import logging
from typing import Optional
from typing import cast

import structlog.stdlib
from tap import Tap

from amarcord.amici.p11.frame_calculation import update_run_frames
from amarcord.amici.p11.frame_calculation import update_runs_add_file_globs
from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.tables import create_tables_from_metadata

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

logger = structlog.stdlib.get_logger(__name__)

_RUN_ID_PLACEHOLDER = "${run_id}"


class Arguments(Tap):
    db_connection_url: str  # Connection URL for the database (e.g. pymysql+mysql://foo/bar)
    # fmt: off
    # pylint: disable=consider-alternative-union-syntax
    file_glob: Optional[str] # String containing the special placeholder ${run_id} which gets replaced by the actual run ID; runs will get a raw_files attributo if this is specified
    # fmt: on

    pool_size: int = 10  # Size of the parallel computation pool for frame calculation (depends on IO usage)


async def _main_loop(args: Arguments) -> None:
    db_context = AsyncDBContext(args.db_connection_url)
    db = AsyncDB(db_context, create_tables_from_metadata(db_context.metadata))

    if args.file_glob is not None:
        if _RUN_ID_PLACEHOLDER not in args.file_glob:
            logger.error(
                f"the file-glob argument should contain the placeholder {_RUN_ID_PLACEHOLDER}, but it doesn't"
            )
            return

        def replacer(run_id: int) -> str:
            return cast(str, args.file_glob).replace(_RUN_ID_PLACEHOLDER, str(run_id))

        async with db.begin() as conn:
            await update_runs_add_file_globs(db, conn, replacer)

    await update_run_frames(db, args.pool_size)


def main() -> None:
    asyncio.run(_main_loop(Arguments(underscores_to_dashes=True).parse_args()))


if __name__ == "__main__":
    main()
