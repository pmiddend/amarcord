import asyncio
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import anyio
import structlog
from sqlalchemy import delete
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from amarcord.db import orm
from amarcord.db.import_export_db import export_db
from amarcord.simple_uri import parse_simple_uri
from amarcord.web.fastapi_utils import get_orm_sessionmaker_with_url

logger = structlog.stdlib.get_logger(__name__)

_SLEEP_DURATION_SECONDS = 10

# If there was an error, to not spam the log with messages, wait a bit
# longer to continue the daemon
_SLEEP_AFTER_ERROR_DURATION_SECONDS = 120


@dataclass(frozen=True)
class ExportImportSettings:
    export_directory: Path | None
    import_directory: Path | None


_AMARCORD_IMPORT_EXPORT_SETTINGS: Final = "AMARCORD_IMPORT_EXPORT_SETTINGS"


def parse_import_export_settings() -> ExportImportSettings:
    s = os.environ.get(_AMARCORD_IMPORT_EXPORT_SETTINGS)
    if s is None:
        return ExportImportSettings(export_directory=None, import_directory=None)
    parsed = parse_simple_uri(s)
    if isinstance(parsed, str):
        raise Exception(
            f'Could not parse environment variable {_AMARCORD_IMPORT_EXPORT_SETTINGS}: Expected something that looks at least like "export:path=/some/path", possibly with more parameters, got "{s}".'
        )
    if parsed.scheme != "export":
        raise Exception(
            f'Could not parse environment variable {_AMARCORD_IMPORT_EXPORT_SETTINGS}: Expected something that looks at least like "export:path=/some/path", possibly with more parameters. The scheme I got was "{parsed.scheme}" though, it must be "export".'
        )
    export_path = parsed.parameters.get("export-path")
    import_path = parsed.parameters.get("import-path")
    return ExportImportSettings(
        export_directory=Path(export_path) if export_path is not None else None,
        import_directory=Path(import_path) if import_path is not None else None,
    )


async def pre_run_cleanup(session: AsyncSession) -> None:
    for job in await session.scalars(
        select(orm.ExportJob).where(
            (orm.ExportJob.started.is_not(None)) & (orm.ExportJob.stopped.is_(None))
        )
    ):
        logger.bind(job_id=job.id).info(
            f'Removing stale export job {job.id} with output path "{job.output_path}".'
        )
        await anyio.Path(job.output_path).unlink(missing_ok=True)
    await session.execute(
        delete(orm.ExportJob).where(
            (orm.ExportJob.started.is_not(None)) & (orm.ExportJob.stopped.is_(None))
        )
    )


async def run_job(db_url: str, job: orm.ExportJob) -> None:
    bound_logger = logger.bind(job_id=job.id)
    bound_logger.info("Starting export job.")

    await export_db(db_url, job.id)


async def daemon(db_url: str) -> None:
    logger.info("Starting export daemon observation loop.")
    async with get_orm_sessionmaker_with_url(db_url)() as session:
        await pre_run_cleanup(session)
    while True:
        try:
            # We start a session to get the jobs, then run "run_job" on
            # every job, giving the DB url instead of the session. This is
            # because exporting can take a long time, and we don't want
            # long-running sessions blocking the DB.
            async with get_orm_sessionmaker_with_url(db_url)() as session:
                unstarted_jobs = list(
                    (
                        await session.scalars(
                            select(orm.ExportJob).where(orm.ExportJob.started.is_(None))
                        )
                    ).all()
                )

            for job in unstarted_jobs:
                await run_job(db_url, job)

                await asyncio.sleep(_SLEEP_DURATION_SECONDS)
        except:
            logger.exception(
                "Error in export daemon loop. Continuing the loop, this might be temporary."
            )
            await asyncio.sleep(_SLEEP_AFTER_ERROR_DURATION_SECONDS)
