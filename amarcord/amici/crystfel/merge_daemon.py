import asyncio
import datetime
import json
from dataclasses import dataclass
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Final

import structlog
from pydantic import BaseModel
from structlog.stdlib import BoundLogger

from amarcord.amici.crystfel.util import make_cell_file_name
from amarcord.amici.crystfel.util import write_cell_file
from amarcord.amici.workload_manager.job import Job
from amarcord.amici.workload_manager.job_status import JobStatus
from amarcord.amici.workload_manager.workload_manager import JobStartError
from amarcord.amici.workload_manager.workload_manager import WorkloadManager
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.db_merge_result import DBMergeResultOutput
from amarcord.db.db_merge_result import DBMergeRuntimeStatus
from amarcord.db.db_merge_result import DBMergeRuntimeStatusDone
from amarcord.db.db_merge_result import DBMergeRuntimeStatusError
from amarcord.db.db_merge_result import DBMergeRuntimeStatusRunning
from amarcord.db.indexing_result import DBIndexingResultDone
from amarcord.db.merge_negative_handling import MergeNegativeHandling
from amarcord.db.merge_result import MergeResult

_RECENT_LOG_LINES: Final = -5

_STDOUT_NAME: Final = "stdout.txt"
_STDERR_NAME: Final = "stderr.txt"

logger = structlog.stdlib.get_logger(__name__)


@dataclass(frozen=True)
class MergeConfig:
    output_base_directory: Path
    merge_script_path: Path
    crystfel_path: Path


def _merge_job_directory(
    config: MergeConfig, merge_result: DBMergeResultOutput
) -> Path:
    return config.output_base_directory / f"merging_{merge_result.id}"


async def start_merge_job(
    parent_logger: BoundLogger,
    workload_manager: WorkloadManager,
    config: MergeConfig,
    merge_result: DBMergeResultOutput,
) -> DBMergeRuntimeStatus:
    parent_logger.info(
        f"starting merge job, indexing results {[ir.id for ir in merge_result.indexing_results]}, point group {merge_result.point_group}"
    )

    finished_results: list[DBIndexingResultDone] = []
    for ir in merge_result.indexing_results:
        if not isinstance(ir.runtime_status, DBIndexingResultDone):
            parent_logger.error(
                f"Indexing result {ir.id} is not finished yet! Status is {ir}"
            )
            return DBMergeRuntimeStatusError(
                error=f"Indexing result {ir.id} is not finished yet! Status is {ir}",
                started=datetime.datetime.utcnow(),
                stopped=datetime.datetime.utcnow(),
                recent_log="",
            )
        finished_results.append(ir.runtime_status)
    parent_logger.info("All indexing results have finished, we can start merging")

    job_base_directory = _merge_job_directory(config, merge_result)
    try:
        job_base_directory.mkdir(parents=True)
        parent_logger.info(f"created job directory {job_base_directory}")
    except:
        parent_logger.exception(f"couldn't create directory {job_base_directory}")
        return DBMergeRuntimeStatusError(
            error=f"couldn't create directory {job_base_directory}",
            started=datetime.datetime.utcnow(),
            stopped=datetime.datetime.utcnow(),
            recent_log="",
        )

    cell_file = job_base_directory / make_cell_file_name(merge_result.cell_description)
    try:
        write_cell_file(
            merge_result.cell_description,
            cell_file,
        )
        parent_logger.info(f"wrote cell file to {cell_file}")
    except Exception as e:
        error_message = f"cell file {cell_file} not writeable: {e}"
        parent_logger.info(error_message)
        return DBMergeRuntimeStatusError(
            error=error_message,
            started=datetime.datetime.utcnow(),
            stopped=datetime.datetime.utcnow(),
            recent_log="",
        )

    command_line_args: list[str] = [
        f"--stream-file={ir.stream_file}" for ir in finished_results
    ] + [
        f"--cell-file={cell_file}",
        f"--crystfel-path={config.crystfel_path}",
        f"--point-group={merge_result.point_group}",
        # We need to escape special characters, I think.
        f'--partialator-additional="{merge_result.partialator_additional}"',
        f"--hkl-file={job_base_directory / 'partialator.hkl'}",
    ]
    if merge_result.negative_handling == MergeNegativeHandling.IGNORE:
        command_line_args.append("--ignore-negs")
    elif merge_result.negative_handling == MergeNegativeHandling.ZERO:
        command_line_args.append("--zero-negs")
    parent_logger.info("command line for this job is " + " ".join(command_line_args))
    try:
        job_start_result = await workload_manager.start_job(
            working_directory=job_base_directory,
            executable=config.merge_script_path,
            command_line=" ".join(command_line_args),
            time_limit=timedelta(days=1),
            stdout=job_base_directory / _STDOUT_NAME,
            stderr=job_base_directory / _STDERR_NAME,
        )

        job_logger = parent_logger.bind(job_id=job_start_result.job_id)
        job_logger.info(f"job start successful, ID {job_start_result.job_id}")
        return DBMergeRuntimeStatusRunning(
            job_id=job_start_result.job_id,
            started=datetime.datetime.utcnow(),
            recent_log="",
        )
    except JobStartError as e:
        logger.error(f"job start errored: {e}")
        return DBMergeRuntimeStatusError(
            error=e.message,
            started=datetime.datetime.utcnow(),
            stopped=datetime.datetime.utcnow(),
            recent_log="",
        )


class MergeResultRootJson(BaseModel):
    error: None | str
    result: None | MergeResult


def _recent_log(config: MergeConfig, merge_result: DBMergeResultOutput) -> None | str:
    stdout_file = _merge_job_directory(config, merge_result) / _STDERR_NAME
    try:
        with stdout_file.open("r", encoding="utf-8") as f:
            return "".join(f.readlines()[_RECENT_LOG_LINES:])
    except:
        return None


def _process_running_job(
    parent_log: BoundLogger,
    config: MergeConfig,
    merge_result: DBMergeResultOutput,
    runtime_status: DBMergeRuntimeStatusRunning,
) -> DBMergeRuntimeStatus:
    log = _recent_log(config, merge_result)
    if log is not None:
        parent_log.info("job is still running, read some output lines")
        return replace(runtime_status, recent_log=log)
    parent_log.info("job is still running, couldn't read log though")
    return None


async def _process_finished_job(
    log: BoundLogger,
    config: MergeConfig,
    merge_result: DBMergeResultOutput,
    runtime_status: DBMergeRuntimeStatusRunning,
) -> DBMergeRuntimeStatus:
    output_file = _merge_job_directory(config, merge_result) / "output.json"
    log.info("job has finished, trying to ingest results")

    new_log = _recent_log(config, merge_result)
    recent_log = new_log if new_log is not None else runtime_status.recent_log

    try:
        with output_file.open("r", encoding="utf-8") as output_file_obj:
            json_result = MergeResultRootJson(**json.load(output_file_obj))
    except:
        log.exception(f"error parsing {output_file}")
        return DBMergeRuntimeStatusError(
            error="error parsing output.json",
            started=runtime_status.started,
            stopped=datetime.datetime.utcnow(),
            recent_log=recent_log,
        )

    if json_result.error is not None:
        log.error(f"error in {output_file}: {json_result.error}")
        return DBMergeRuntimeStatusError(
            error=json_result.error,
            started=runtime_status.started,
            stopped=datetime.datetime.utcnow(),
            recent_log=recent_log,
        )

    if json_result.result is None:
        error_message = f"{output_file}: both error and result in output.json are unset; fix the script"
        log.error(error_message)
        return DBMergeRuntimeStatusError(
            error=error_message,
            started=runtime_status.started,
            stopped=datetime.datetime.utcnow(),
            recent_log=recent_log,
        )

    log.info(f"{output_file}: ingested figures of merit")
    return DBMergeRuntimeStatusDone(
        started=runtime_status.started,
        stopped=datetime.datetime.utcnow(),
        result=json_result.result,
        recent_log=recent_log,
    )


async def _merging_loop_iteration(
    db: AsyncDB, workload_manager: WorkloadManager, config: MergeConfig
) -> None:
    async with db.read_only_connection() as conn:
        merge_results = await db.retrieve_merge_results(conn)

    jobs_on_workload_manager = {j.id: j for j in await workload_manager.list_jobs()}
    for merge_result in merge_results:
        result_logger = logger.bind(merge_result_id=merge_result.id)
        new_status = await _merging_loop_single_result(
            config,
            jobs_on_workload_manager,
            merge_result,
            result_logger,
            workload_manager,
        )
        if new_status is not None:
            async with db.begin() as conn:
                if isinstance(new_status, DBMergeRuntimeStatusDone) and isinstance(
                    new_status.result.mtz_file, Path
                ):
                    job_directory = _merge_job_directory(config, merge_result)
                    real_mtz_path = job_directory / new_status.result.mtz_file
                    try:
                        file_result = await db.create_file(
                            conn,
                            real_mtz_path.name,
                            description="MTZ output",
                            original_path=real_mtz_path,
                            contents_location=real_mtz_path,
                            deduplicate=False,
                        )
                    except:
                        result_logger.exception("error uploading MTZ file to DB")
                        await db.update_merge_result_status(
                            conn,
                            merge_result.id,
                            DBMergeRuntimeStatusError(
                                "Error uploading MTZ file to DB",
                                new_status.started,
                                new_status.stopped,
                                new_status.recent_log,
                            ),
                        )
                    await db.update_merge_result_status(
                        conn,
                        merge_result.id,
                        DBMergeRuntimeStatusDone(
                            started=new_status.started,
                            stopped=new_status.stopped,
                            result=MergeResult(
                                mtz_file=file_result.id,
                                fom=new_status.result.fom,
                                detailed_foms=new_status.result.detailed_foms,
                            ),
                            recent_log=new_status.recent_log,
                        ),
                    )
                else:
                    await db.update_merge_result_status(
                        conn, merge_result.id, new_status
                    )


async def _merging_loop_single_result(
    config: MergeConfig,
    jobs_on_workload_manager: dict[int, Job],
    merge_result: DBMergeResultOutput,
    result_logger: BoundLogger,
    workload_manager: WorkloadManager,
) -> DBMergeRuntimeStatus:
    # Nothing to do for jobs that are done (we could filter them via SQL beforehand if it's too much to handle).
    if isinstance(
        merge_result.runtime_status,
        (DBMergeRuntimeStatusDone, DBMergeRuntimeStatusError),
    ):
        return None
    if merge_result.runtime_status is None:
        return await start_merge_job(
            result_logger, workload_manager, config, merge_result
        )
    assert isinstance(merge_result.runtime_status, DBMergeRuntimeStatusRunning)
    job_logger = result_logger.bind(job_id=merge_result.runtime_status.job_id)
    workload_job = jobs_on_workload_manager.get(merge_result.runtime_status.job_id)
    if workload_job is None or workload_job.status in (
        JobStatus.FAILED,
        JobStatus.SUCCESSFUL,
    ):
        if workload_job is None:
            result_logger.info(
                f"finished because not in SLURM REST job list anymore (available jobs: {jobs_on_workload_manager.keys()})"
            )
        else:
            result_logger.info(
                f"finished because SLURM REST job status is {workload_job.status}"
            )
        # Job has finished somehow (could be erroneous)
        return await _process_finished_job(
            job_logger, config, merge_result, merge_result.runtime_status
        )
    return _process_running_job(
        job_logger, config, merge_result, merge_result.runtime_status
    )


async def merging_loop(
    db: AsyncDB,
    config: MergeConfig,
    workload_manager: WorkloadManager,
    sleep_seconds: float,
) -> None:
    logger.info("starting CrystFEL merging loop")
    while True:
        await _merging_loop_iteration(db, workload_manager, config)
        await asyncio.sleep(sleep_seconds)
