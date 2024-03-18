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
from amarcord.amici.crystfel.util import determine_output_directory
from amarcord.amici.workload_manager.job_status import JobStatus
from amarcord.amici.workload_manager.workload_manager import JobStartError
from amarcord.amici.workload_manager.workload_manager import WorkloadManager
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.db_job_status import DBJobStatus
from amarcord.db.indexing_result import DBIndexingResultDone
from amarcord.db.indexing_result import DBIndexingResultOutput
from amarcord.db.indexing_result import DBIndexingResultRunning
from amarcord.db.indexing_result import DBIndexingResultRuntimeStatus
from amarcord.db.indexing_result import empty_indexing_fom
from amarcord.db.run_internal_id import RunInternalId
from amarcord.db.table_classes import BeamtimeOutput
from amarcord.db.table_classes import DBRunOutput

logger = structlog.stdlib.get_logger(__name__)

_LONG_BREAK_DURATION_SECONDS = 5
_SHORT_BREAK_DURATION_SECONDS = 1


@dataclass(frozen=True)
class CrystFELOnlineConfig:
    output_base_directory: Path
    beamtime_id: None | BeamtimeId
    crystfel_path: Path
    api_url: str
    asapo_source: str
    cpu_count_multiplier: None | float
    use_auto_geom_refinement: bool
    dummy_h5_input: None | str


async def start_indexing_job(
    bound_logger: BoundLogger,
    workload_manager: WorkloadManager,
    config: CrystFELOnlineConfig,
    beamtime: BeamtimeOutput,
    run: DBRunOutput,
    indexing_result: DBIndexingResultOutput,
) -> DBIndexingResultRuntimeStatus:
    bound_logger.info("starting indexing job")

    job_base_directory = determine_output_directory(
        beamtime, config.output_base_directory, {}
    )

    output_base_name = f"run-{run.external_id}-indexing-{indexing_result.id}"
    stream_file = job_base_directory / f"{output_base_name}.stream"

    try:
        with Path(inspect.getfile(amarcord.cli.crystfel_index)).open(
            "r", encoding="utf-8"
        ) as merge_file:
            predefined_args = {
                # We could give CrystFEL the internal ID as well, and
                # it wouldn't matter, but the user expects the
                # external, beamtime-specific one
                "run-id": run.external_id,
                "job-id": indexing_result.id,
                "asapo-source": config.asapo_source,
                "cpu-count-multiplier": (
                    config.cpu_count_multiplier if config.cpu_count_multiplier else 2.0
                ),
                "api-url": config.api_url,
                "stream-file": str(stream_file),
                "dummy-h5-input": config.dummy_h5_input,
                "crystfel-path": str(config.crystfel_path),
                "use-auto-geom-refinement": config.use_auto_geom_refinement,
                "cell-description": (
                    coparse_cell_description(indexing_result.cell_description)
                    if indexing_result.cell_description is not None
                    else None
                ),
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
                working_directory=job_base_directory,
                name=f"ix_run_{run.external_id}",
                script=indexing_file_contents,
                time_limit=timedelta(days=1),
                stdout=job_base_directory / f"{output_base_name}-stdout.txt",
                stderr=job_base_directory / f"{output_base_name}-stderr.txt",
            )
            bound_logger.info(
                "job start successful", indexing_job_id=job_start_result.job_id
            )
            return DBIndexingResultRunning(
                stream_file=stream_file,
                job_id=job_start_result.job_id,
                fom=empty_indexing_fom,
            )
    except JobStartError as e:
        bound_logger.error(f"job start errored: {e}")
        return DBIndexingResultDone(
            stream_file=stream_file,
            job_error=e.message,
            fom=empty_indexing_fom,
        )


async def _start_new_jobs(
    db: AsyncDB, workload_manager: WorkloadManager, config: CrystFELOnlineConfig
) -> None:
    runs: dict[RunInternalId, DBRunOutput] = {}
    async with db.read_only_connection() as conn:
        queued_indexing_results = await db.retrieve_indexing_results(
            conn, beamtime_id=config.beamtime_id, job_status_filter=DBJobStatus.QUEUED
        )
        attributi = await db.retrieve_attributi(
            conn, beamtime_id=None, associated_table=None
        )
        for indexing_result in queued_indexing_results:
            runs[indexing_result.run_id] = await db.retrieve_run(
                conn, internal_id=indexing_result.run_id, attributi=attributi
            )
        beamtimes = {bt.id: bt for bt in await db.retrieve_beamtimes(conn)}

    if queued_indexing_results:
        for indexing_result in queued_indexing_results:
            bound_logger = logger.bind(
                run_id=indexing_result.run_id, indexing_result_id=indexing_result.id
            )
            run = runs.get(indexing_result.run_id)
            if run is None:
                raise Exception(
                    f"indexing result {indexing_result.id} has run ID {indexing_result.run_id} which we cannot find in our list of run IDs:"
                    + ", ".join(str(s) for s in runs)
                )
            beamtime = beamtimes.get(run.beamtime_id)
            if beamtime is None:
                raise Exception(
                    f"indexing result {indexing_result.id} has run {indexing_result.run_id} which has beamtime {run.beamtime_id} we cannot find in our list of beamtime IDs:"
                    + ", ".join(str(s) for s in beamtimes.keys())
                )
            new_status = await start_indexing_job(
                bound_logger, workload_manager, config, beamtime, run, indexing_result
            )

            async with db.begin() as conn:
                await db.update_indexing_result_status(
                    conn,
                    indexing_result_id=indexing_result.id,
                    runtime_status=new_status,
                )

            bound_logger.info(
                f"new indexing job submitted, taking a {_LONG_BREAK_DURATION_SECONDS}s break"
            )
            await asyncio.sleep(_LONG_BREAK_DURATION_SECONDS)
    else:
        await asyncio.sleep(_SHORT_BREAK_DURATION_SECONDS)


def _process_premature_finish(
    bound_logger: BoundLogger,
    runtime_status: DBIndexingResultRuntimeStatus,
) -> DBIndexingResultRuntimeStatus:
    bound_logger.info("job has finished prematurely")

    return DBIndexingResultDone(
        job_error="job has finished on SLURM, but delivered no results",
        fom=runtime_status.fom,  # type: ignore
        stream_file=runtime_status.stream_file,  # type: ignore
    )


async def _update_jobs(
    db: AsyncDB, workload_manager: WorkloadManager, beamtime_id: None | BeamtimeId
) -> None:
    jobs_on_workload_manager = {j.id: j for j in await workload_manager.list_jobs()}

    async with db.read_only_connection() as conn:
        db_jobs = await db.retrieve_indexing_results(
            conn, beamtime_id=beamtime_id, job_status_filter=DBJobStatus.RUNNING
        )

    for indexing_result in db_jobs:
        if not isinstance(indexing_result.runtime_status, DBIndexingResultRunning):
            continue

        bound_logger = logger.bind(
            indexing_job_id=indexing_result.runtime_status.job_id,
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
            bound_logger.info("finished because not in SLURM REST job list anymore")
        else:
            bound_logger.info(
                f"finished because SLURM REST job status is {workload_job.status}"
            )

        # Job has finished prematurely
        new_status = _process_premature_finish(
            bound_logger, indexing_result.runtime_status
        )

        async with db.begin() as conn:
            await db.update_indexing_result_status(conn, indexing_result.id, new_status)

    logger.info("indexing jobs stati updated, take a (longer) break")
    await asyncio.sleep(_LONG_BREAK_DURATION_SECONDS)


async def _indexing_loop_iteration(
    db: AsyncDB,
    workload_manager: WorkloadManager,
    config: CrystFELOnlineConfig,
    start_new_jobs: bool,
) -> None:
    # This is weird, I know, but it stems from the fact that the DESY Maxwell REST API has rate limiting included,
    # so we cannot just do two REST API requests back to back. Instead, we have this weird counter.
    if start_new_jobs:
        await _start_new_jobs(db, workload_manager, config)

    else:
        await _update_jobs(db, workload_manager, config.beamtime_id)


async def indexing_loop(
    db: AsyncDB,
    workload_manager: WorkloadManager,
    config: CrystFELOnlineConfig,
) -> None:
    logger.info("starting Online CrystFEL indexing loop")

    counter = 0
    while True:
        await _indexing_loop_iteration(db, workload_manager, config, counter % 30 != 0)

        counter += 1
