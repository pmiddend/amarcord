import asyncio
from pathlib import Path

import anyio
import structlog
import typed_argparse

from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.import_export_db import import_db
from amarcord.db.import_export_db import import_db_from_zip_file
from amarcord.logging_util import setup_structlog
from amarcord.web.fastapi_utils import get_orm_sessionmaker_with_url

setup_structlog()

logger = structlog.stdlib.get_logger(__name__)


class FromFileArgs(typed_argparse.TypedArgs):
    file_name: Path = typed_argparse.arg(help="Name of the (.zip) file to import from")
    stream_file_output_dir: Path = typed_argparse.arg(
        help="Where to place imported .stream files (directory must exist!)"
    )
    import_to_db_connection_url: str = typed_argparse.arg(
        help="Connection URL for the database to import to (e.g. sqlite+aiosqlite://foo/bar)"
    )
    new_title: str | None = typed_argparse.arg(
        help="New title for the beamtime (will be the same as old title if omitted)"
    )


class FromDbArgs(typed_argparse.TypedArgs):
    import_from_db_connection_url: str = typed_argparse.arg(
        help="Connection URL for the database to import from (e.g. sqlite+aiosqlite://foo/bar)"
    )
    import_to_db_connection_url: str = typed_argparse.arg(
        help="Connection URL for the database to import to (e.g. sqlite+aiosqlite://foo/bar)"
    )
    beamtime_id: int = typed_argparse.arg(help="Beamtime ID to import from")
    new_title: str | None = typed_argparse.arg(
        help="New title for the beamtime (will be the same as old title if omitted)"
    )
    new_output_path: str | None = typed_argparse.arg(
        help="New analysis output path for the beamtime (will be unchanged if this parameter is omitted)"
    )


async def _import_db_outer(args: FromDbArgs) -> None:
    async with (
        get_orm_sessionmaker_with_url(
            args.import_to_db_connection_url
        )() as import_into_session,
        get_orm_sessionmaker_with_url(
            args.import_from_db_connection_url
        )() as import_from_session,
    ):
        new_beamtime_id = await import_db(
            BeamtimeId(args.beamtime_id),
            import_into_session,
            import_from_session,
            new_title=args.new_title,
            new_output_path=args.new_output_path,
        )
        await import_into_session.commit()
        logger.info(
            f"Merged beamtime {args.beamtime_id}. New beamtime ID in target database is {new_beamtime_id}."
        )


async def _import_file_outer(args: FromFileArgs) -> None:
    await import_db_from_zip_file(
        Path(args.file_name),
        anyio.Path(args.stream_file_output_dir),
        args.import_to_db_connection_url,
    )
    logger.info(f'Import from zip file "{args.file_name}" completed succesfully.')


def main() -> None:
    def import_db_wrapper(args: FromDbArgs) -> None:
        asyncio.run(_import_db_outer(args))

    def import_file_wrapper(args: FromFileArgs) -> None:
        asyncio.run(_import_file_outer(args))

    typed_argparse.Parser(
        typed_argparse.SubParserGroup(
            typed_argparse.SubParser(
                "import-from-db",
                FromDbArgs,
                help="Import a single beamtime from a different, accessible AMARCORD instance",
            ),
            typed_argparse.SubParser(
                "import-from-file",
                FromFileArgs,
                help="Import all beamtimes from a .zip file exported from a different AMARCORD instance",
            ),
        )
    ).bind(import_db_wrapper, import_file_wrapper).run()


if __name__ == "__main__":
    main()
