import asyncio
import inspect
import json
from base64 import b64encode
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

import structlog
from structlog.stdlib import BoundLogger

import amarcord.cli.crystfel_index
from amarcord.amici.crystfel.util import coparse_cell_description
from amarcord.amici.workload_manager.job_status import JobStatus
from amarcord.amici.workload_manager.workload_manager import JobStartError
from amarcord.amici.workload_manager.workload_manager import WorkloadManager
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.attributo_id import AttributoId
from amarcord.db.db_job_status import DBJobStatus
from amarcord.db.indexing_result import DBIndexingResultDone
from amarcord.db.indexing_result import DBIndexingResultOutput
from amarcord.db.indexing_result import DBIndexingResultRunning
from amarcord.db.indexing_result import DBIndexingResultRuntimeStatus
from amarcord.db.indexing_result import empty_indexing_fom

logger = structlog.stdlib.get_logger(__name__)

ATTRIBUTO_POINT_GROUP = AttributoId("point group")
ATTRIBUTO_CELL_DESCRIPTION = AttributoId("cell description")


@dataclass(frozen=True)
class CrystFELOnlineConfig:
    output_base_directory: Path
    crystfel_path: Path
    api_url: str
    use_auto_geom_refinement: bool
    dummy_h5_input: None | str


async def start_indexing_job(
    workload_manager: WorkloadManager,
    config: CrystFELOnlineConfig,
    indexing_result: DBIndexingResultOutput,
) -> DBIndexingResultRuntimeStatus:
    bound_logger = logger.bind(
        indexing_result_id=indexing_result.id, run_id=indexing_result.run_id
    )
    bound_logger.info("starting indexing job")

    job_base_directory = config.output_base_directory
    output_base_name = f"run_{indexing_result.run_id}_indexing_{indexing_result.id}"
    stream_file = job_base_directory / "processed" / f"{output_base_name}.stream"

    try:
        with Path(inspect.getfile(amarcord.cli.crystfel_index)).open(
            "r", encoding="utf-8"
        ) as merge_file:
            predefined_args = {
                "run-id": indexing_result.run_id,
                "job-id": indexing_result.id,
                "api-url": config.api_url,
                "stream-file": str(stream_file),
                "dummy-h5-input": config.dummy_h5_input,
                "crystfel-path": str(config.crystfel_path),
                "use-auto-geom-refinement": config.use_auto_geom_refinement,
                "cell-description": coparse_cell_description(
                    indexing_result.cell_description
                )
                if indexing_result.cell_description is not None
                else None,
            }
            bound_logger.info(
                "command line for this job is " + " ".join(predefined_args)
            )
            predefined_args_b64 = b64encode(
                json.dumps(predefined_args, allow_nan=False).encode("utf-8")
            ).decode("utf-8")
            indexing_file_contents = merge_file.read().replace(
                "predefined_args: None | bytes = None",
                f'predefined_args = "{predefined_args_b64}"',
            )
            job_start_result = await workload_manager.start_job(
                working_directory=config.output_base_directory,
                script=indexing_file_contents,
                time_limit=timedelta(days=1),
                stdout=config.output_base_directory
                / "processed"
                / "logs"
                / f"indexing_{indexing_result.id}_stdout.txt",
                stderr=config.output_base_directory
                / "processed"
                / "logs"
                / f"indexing_{indexing_result.id}_stderr.txt",
            )
            logger.info(f"job start successful, ID {job_start_result.job_id}")
            return DBIndexingResultRunning(
                stream_file=stream_file,
                job_id=job_start_result.job_id,
                fom=empty_indexing_fom,
            )
    except JobStartError as e:
        logger.error(f"job start errored: {e}")
        return DBIndexingResultDone(
            stream_file=stream_file,
            job_error=e.message,
            fom=empty_indexing_fom,
        )


async def _start_new_jobs(
    db: AsyncDB, workload_manager: WorkloadManager, config: CrystFELOnlineConfig
) -> None:

    async with db.read_only_connection() as conn:
        queued_indexing_results = await db.retrieve_indexing_results(
            conn, DBJobStatus.QUEUED
        )

    if queued_indexing_results:
        for indexing_result in queued_indexing_results:
            logger.info(f"starting indexing job for run {indexing_result.run_id}")
            new_status = await start_indexing_job(
                workload_manager, config, indexing_result
            )

            async with db.begin() as conn:
                await db.update_indexing_result_status(
                    conn,
                    indexing_result_id=indexing_result.id,
                    runtime_status=new_status,
                )

            logger.info("new indexing job submitted, taking a long break")
            await asyncio.sleep(5)
    else:
        await asyncio.sleep(1)


def _process_premature_finish(
    log: BoundLogger,
    runtime_status: DBIndexingResultRuntimeStatus,
) -> DBIndexingResultRuntimeStatus:
    log.info("job has finished prematurely")

    return DBIndexingResultDone(
        job_error="job has finished on SLURM, but delivered no results",
        fom=runtime_status.fom,  # type: ignore
        stream_file=runtime_status.stream_file,  # type: ignore
    )


async def _update_jobs(db: AsyncDB, workload_manager: WorkloadManager) -> None:

    jobs_on_workload_manager = {j.id: j for j in await workload_manager.list_jobs()}

    async with db.read_only_connection() as conn:
        db_jobs = await db.retrieve_indexing_results(conn, DBJobStatus.RUNNING)

    for indexing_result in db_jobs:
        if not isinstance(indexing_result.runtime_status, DBIndexingResultRunning):
            continue

        log = logger.bind(
            job_id=indexing_result.runtime_status.job_id,
            run_id=indexing_result.run_id,
        )

        workload_job = jobs_on_workload_manager.get(
            indexing_result.runtime_status.job_id
        )
        if workload_job is not None and workload_job.status not in (
            JobStatus.FAILED,
            JobStatus.SUCCESSFUL,
        ):
            # Running job, let it keep running
            continue

        if workload_job is None:
            log.info("finished because not in SLURM REST job list anymore")
        else:
            log.info(f"finished because SLURM REST job status is {workload_job.status}")

        # Job has finished prematurely
        new_status = _process_premature_finish(log, indexing_result.runtime_status)

        async with db.begin() as conn:
            await db.update_indexing_result_status(conn, indexing_result.id, new_status)

    logger.info("indexing jobs stati updated, take a (longer) break")
    await asyncio.sleep(5)


async def _indexing_loop_iteration(
    db: AsyncDB,
    workload_manager: WorkloadManager,
    config: CrystFELOnlineConfig,
    start_new_jobs: bool,
) -> None:

    if start_new_jobs:
        await _start_new_jobs(db, workload_manager, config)

    else:
        await _update_jobs(db, workload_manager)


async def indexing_loop(
    db: AsyncDB, workload_manager: WorkloadManager, config: CrystFELOnlineConfig
) -> None:
    logger.info("starting Online CrystFEL indexing loop")

    counter = 0
    while True:
        async with db.read_only_connection() as conn:
            user_config = await db.retrieve_configuration(conn)

        if user_config.use_online_crystfel:
            await _indexing_loop_iteration(
                db, workload_manager, config, counter % 30 != 0
            )

        counter += 1
