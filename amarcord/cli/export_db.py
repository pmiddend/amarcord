import asyncio
import datetime

import structlog
import typed_argparse as tap

from amarcord.db import orm
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.import_export_db import export_db
from amarcord.logging_util import setup_structlog
from amarcord.web.fastapi_utils import get_orm_sessionmaker_with_url

setup_structlog()

logger = structlog.stdlib.get_logger(__name__)


class Arguments(tap.TypedArgs):
    db_connection_url: str = tap.arg(
        help="Connection URL for the database to export from (e.g. mysql+pymysql://foo/bar"
    )
    export_beamtime_id: int = tap.arg(help="Beamtime ID to export from")
    with_stream_files: bool = tap.arg(help="Include .stream files, too?")
    output_zip_file: str = tap.arg(help="File to save to")


async def _export_db_outer(args: Arguments) -> None:
    async with get_orm_sessionmaker_with_url(args.db_connection_url)() as session:
        beamtime = await session.get(orm.Beamtime, args.export_beamtime_id)
        if beamtime is None:
            logger.error(
                f"Found no beamtime with ID {args.export_beamtime_id}, cannot export."
            )
            return
        new_job = orm.ExportJob(
            beamtime_id=BeamtimeId(args.export_beamtime_id),
            created=datetime.datetime.now(datetime.UTC),
            started=None,
            stopped=None,
            size_in_mebibytes=0,
            output_path=args.output_zip_file,
            status_message="",
            contains_stream_files=args.with_stream_files,
        )
        session.add(new_job)
        await session.commit()

    logger.info(f"Added new export job with ID {new_job.id}, starting this job now.")
    await export_db(
        args.db_connection_url,
        new_job.id,
    )
    logger.info(f'Export done, result is stored in "{args.output_zip_file}".')


def main() -> None:
    def run(args: Arguments) -> None:
        asyncio.run(_export_db_outer(args))

    tap.Parser(Arguments).bind(run).run()


if __name__ == "__main__":
    main()
