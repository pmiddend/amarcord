# pylint: disable=unused-argument,unused-import
import asyncio
import logging
from pathlib import Path
from time import sleep
from typing import Optional, List

from tap import Tap

from amarcord.amici.crystfel.project_parser import (
    parse_crystfel_project_file,
    CrystfelProjectFile,
)
from amarcord.amici.slurm.job import Job
from amarcord.amici.slurm.job_controller import JobController
from amarcord.amici.slurm.job_controller_factory import (
    create_job_controller,
    parse_job_controller,
)
from amarcord.amici.slurm.job_status import JobStatus
from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.dbcontext import CreationMode, Connection
from amarcord.db.indexing_job import DBIndexingJob
from amarcord.db.job_status import DBJobStatus
from amarcord.db.tables import create_tables_from_metadata

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)


class Arguments(Tap):
    db_connection_url: str  # Connection URL for the database (e.g. pymysql+mysql://foo/bar)
    proposal_id: int  # proposal ID to use
    wait_time_seconds: float = 10.0  # How much to wait between analysis runs
    project_file_path: Path  # Path to the CrystFEL project file
    verbose: bool  # Raise log level to DEBUG
    debug: bool  # Creates a database if non exists
    job_controller_url: str


def mymain(args: Arguments) -> None:
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    dbcontext = AsyncDBContext(args.db_connection_url, echo=False)
    db = AsyncDB(dbcontext, create_tables_from_metadata(dbcontext.metadata))

    if args.debug:
        dbcontext.create_all(CreationMode.CHECK_FIRST)

    job_controller = create_job_controller(
        parse_job_controller(args.job_controller_url)
    )
    while True:
        try:
            asyncio.run(main_loop_iteration(args, db, job_controller))
        except:
            logger.exception("Loop iteration exception, waiting and then continuing...")

        logger.info("waiting for next check")
        sleep(args.wait_time_seconds)


async def _start_indexing_job(
    conn: Connection,
    run_id: int,
    project_file: CrystfelProjectFile,
    args: Arguments,
    job_controller: JobController,
) -> None:
    # FIXME: convert project file to TOML File, supplementing run ID and args to make it complete.
    # Use job controller (pass in) to start SLURM job for Xwiz, and enter SLURM job ID.
    # If starting it didn't work, add delay (maybe just inside the daemon with a cache).
    pass


async def _start_new_jobs(
    args: Arguments,
    db: AsyncDB,
    project_file: CrystfelProjectFile,
    job_controller: JobController,
) -> None:
    with db.connect() as conn:
        for run_id in await db.retrieve_runs_without_indexing_jobs(conn):
            await _start_indexing_job(conn, run_id, project_file, args, job_controller)


async def _complete_indexing_job(conn: Connection, j: DBIndexingJob) -> None:
    # FIXME: parse Xwiz output and set job status to completion (or maybe update percent)
    pass


async def _update_indexing_job(conn: Connection, j: DBIndexingJob) -> None:
    # FIXME: parse Xwiz output and set job percentage
    pass


async def _observe_started_jobs(
    args: Arguments,
    db: AsyncDB,
    job_controller: JobController,
) -> None:
    controller_jobs = list(job_controller.list_jobs())

    with db.connect() as conn:
        for db_job in await db.retrieve_indexing_jobs(conn, [DBJobStatus.RUNNING]):
            await _process_running_indexing_job(
                conn, controller_jobs, db_job, job_controller
            )


async def _process_running_indexing_job(
    conn: Connection,
    controller_jobs: List[Job],
    db_job: DBIndexingJob,
    job_controller: JobController,
):
    controller_job: Optional[Job] = None
    for controller_job_candidate in controller_jobs:
        if job_controller.equals(db_job.metadata, controller_job_candidate.metadata):
            controller_job = controller_job_candidate
            break
    if controller_job is None or controller_job.status in [
        JobStatus.COMPLETED,
        JobStatus.FAILED,
    ]:
        logger.debug(f"job {db_job.id}: completed")
        await _complete_indexing_job(conn, db_job)
    else:
        logger.debug(f"job {db_job.id}: still running, updating percent")
        await _update_indexing_job(conn, db_job)


async def main_loop_iteration(
    args: Arguments, db: AsyncDB, job_controller: JobController
) -> None:
    if not args.project_file_path.is_file():
        logger.debug(
            f"project file {args.project_file_path} not a file, doing nothing..."
        )
        return
    try:
        await _start_new_jobs(
            args,
            db,
            parse_crystfel_project_file(args.project_file_path),
            job_controller,
        )
    except:
        logger.exception(f"project file {args.project_file_path}: cannot parse")
        return

    await _observe_started_jobs(args, db, job_controller)


if __name__ == "__main__":
    mymain(Arguments(underscores_to_dashes=True).parse_args())
