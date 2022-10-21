import asyncio
import datetime
import json
from dataclasses import dataclass
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Final
from typing import cast

import structlog
from pydantic import BaseModel
from structlog.stdlib import BoundLogger

from amarcord.amici.crystfel.util import make_cell_file_name
from amarcord.amici.crystfel.util import write_cell_file
from amarcord.amici.workload_manager.job_status import JobStatus
from amarcord.amici.workload_manager.workload_manager import JobStartError
from amarcord.amici.workload_manager.workload_manager import WorkloadManager
from amarcord.db.async_dbcontext import Connection
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.db_job_status import DBJobStatus
from amarcord.db.db_merge_result import DBMergeResultOutput
from amarcord.db.db_merge_result import DBMergeRuntimeStatus
from amarcord.db.db_merge_result import DBMergeRuntimeStatusDone
from amarcord.db.db_merge_result import DBMergeRuntimeStatusError
from amarcord.db.db_merge_result import DBMergeRuntimeStatusRunning
from amarcord.db.indexing_result import DBIndexingResultDone
from amarcord.db.merge_negative_handling import MergeNegativeHandling
from amarcord.db.merge_result import MergeResult
from amarcord.db.merge_result import RefinementResult
from amarcord.db.table_classes import DBFile

_RECENT_LOG_LINES: Final = -5

_STDOUT_NAME: Final = "stdout.txt"
_STDERR_NAME: Final = "stderr.txt"
_MAXIMUM_RECENT_LOG_LENGTH: Final = 4096

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


def _find_file_id_by_extension(files: list[DBFile], extension: str) -> None | int:
    for f in files:
        if f.file_name.endswith(f".{extension}"):
            return f.id
    return None


async def start_merge_job(
    db: AsyncDB,
    conn: Connection,
    parent_logger: BoundLogger,
    workload_manager: WorkloadManager,
    config: MergeConfig,
    merge_result: DBMergeResultOutput,
) -> DBMergeRuntimeStatus:
    parent_logger.info(
        f"starting merge job, indexing results {[ir.id for ir in merge_result.indexing_results]}, point group {merge_result.point_group}"
    )

    finished_results: list[DBIndexingResultDone] = []
    pdb_file_id: None | int = None
    mtz_file_id: None | int = None
    attributi = await db.retrieve_attributi(conn, associated_table=None)
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
        chemical = await db.retrieve_chemical(conn, ir.chemical_id, attributi)
        if chemical is None:
            return DBMergeRuntimeStatusError(
                error=f"chemical {ir.chemical_id} not found",
                started=datetime.datetime.utcnow(),
                stopped=datetime.datetime.utcnow(),
                recent_log="",
            )
        this_pdb_file_id = _find_file_id_by_extension(chemical.files, "pdb")
        this_mtz_file_id = _find_file_id_by_extension(chemical.files, "mtz")
        if this_pdb_file_id is not None:
            pdb_file_id = this_pdb_file_id
        if this_mtz_file_id is not None:
            mtz_file_id = this_mtz_file_id
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
    if pdb_file_id is not None:
        pdb_file = await db.retrieve_file(conn, pdb_file_id, with_contents=True)
        pdb_path = job_base_directory / "base-model.pdb"
        with pdb_path.open("wb") as pdb_file_object:
            pdb_file_object.write(cast(bytes, pdb_file.contents))
        command_line_args.append(f"--pdb={pdb_path}")
        if mtz_file_id is not None:
            mtz_file = await db.retrieve_file(conn, mtz_file_id, with_contents=True)
            mtz_path = job_base_directory / "rfree.mtz"
            with mtz_path.open("wb") as mtz_file_object:
                mtz_file_object.write(cast(bytes, mtz_file.contents))
            command_line_args.append(f"--rfree-mtz={mtz_path}")
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
            return "".join(f.readlines()[_RECENT_LOG_LINES:])[
                0:_MAXIMUM_RECENT_LOG_LENGTH
            ]
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


async def _start_new_jobs(
    db: AsyncDB, workload_manager: WorkloadManager, config: MergeConfig
) -> None:
    async with db.read_only_connection() as conn:
        merge_results = await db.retrieve_merge_results(conn, DBJobStatus.QUEUED)

    if merge_results:
        for merge_result in merge_results:
            result_logger = logger.bind(merge_result_id=merge_result.id)
            result_logger.info("starting merge job")

            async with db.begin() as conn:
                new_status = await start_merge_job(
                    db, conn, result_logger, workload_manager, config, merge_result
                )
                await db.update_merge_result_status(conn, merge_result.id, new_status)

            logger.info("new merge job submitted, taking a long break")
            await asyncio.sleep(20)
    else:
        logger.info("no new merge jobs to submit, take a break")
        await asyncio.sleep(1)


async def _update_jobs(
    db: AsyncDB, workload_manager: WorkloadManager, config: MergeConfig
) -> None:
    jobs_on_workload_manager = {j.id: j for j in await workload_manager.list_jobs()}

    async with db.read_only_connection() as conn:
        db_jobs = await db.retrieve_merge_results(conn, DBJobStatus.RUNNING)

    for merge_result in db_jobs:
        result_logger = logger.bind(merge_result_id=merge_result.id)

        if not isinstance(merge_result.runtime_status, DBMergeRuntimeStatusRunning):
            continue

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
            new_status = await _process_finished_job(
                job_logger, config, merge_result, merge_result.runtime_status
            )
        else:
            new_status = _process_running_job(
                job_logger, config, merge_result, merge_result.runtime_status
            )

        if new_status is None:
            continue

        async with db.begin() as conn:
            if isinstance(new_status, DBMergeRuntimeStatusDone):
                for rr in new_status.result.refinement_results:
                    await upload_refinement_result(
                        conn,
                        db,
                        config,
                        merge_result,
                        new_status,
                        result_logger,
                        rr,
                    )
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
                            refinement_results=new_status.result.refinement_results,
                            detailed_foms=new_status.result.detailed_foms,
                        ),
                        recent_log=new_status.recent_log,
                    ),
                )
            else:
                await db.update_merge_result_status(conn, merge_result.id, new_status)

    logger.info("merge jobs stati updated, take a (longer) break")
    await asyncio.sleep(20)


async def upload_refinement_result(
    conn: Connection,
    db: AsyncDB,
    config: MergeConfig,
    merge_result: DBMergeResultOutput,
    new_status: DBMergeRuntimeStatusDone,
    result_logger: BoundLogger,
    rr: RefinementResult,
) -> None | int:
    job_directory = _merge_job_directory(config, merge_result)
    if not isinstance(rr.mtz, Path) or not isinstance(rr.pdb, Path):
        return None
    mtz_path = job_directory / rr.mtz
    pdb_path = job_directory / rr.pdb
    try:
        refinement_mtz_result = (
            await db.create_file(
                conn,
                mtz_path.name,
                description="MTZ output",
                original_path=mtz_path,
                contents_location=mtz_path,
                deduplicate=False,
            )
        ).id
    except:
        result_logger.exception("error uploading refinement MTZ file to DB")
        await db.update_merge_result_status(
            conn,
            merge_result.id,
            DBMergeRuntimeStatusError(
                "Error uploading refinement MTZ file to DB",
                new_status.started,
                new_status.stopped,
                new_status.recent_log,
            ),
        )
        return None
    try:
        refinement_pdb_result = (
            await db.create_file(
                conn,
                pdb_path.name,
                description="PDB output",
                original_path=pdb_path,
                contents_location=pdb_path,
                deduplicate=False,
            )
        ).id
    except:
        result_logger.exception("error uploading refinement PDB file to DB")
        await db.update_merge_result_status(
            conn,
            merge_result.id,
            DBMergeRuntimeStatusError(
                "Error uploading refinement PDB file to DB",
                new_status.started,
                new_status.stopped,
                new_status.recent_log,
            ),
        )
        return None
    return await db.create_refinement_result(
        conn,
        merge_result.id,
        refinement_pdb_result,
        refinement_mtz_result,
        rr.r_free,
        rr.r_work,
        rr.rms_bond_angle,
        rr.rms_bond_length,
    )


async def _merging_loop_iteration(
    db: AsyncDB,
    workload_manager: WorkloadManager,
    config: MergeConfig,
    start_new_jobs: bool,
) -> None:
    if start_new_jobs:
        logger.info("starting new merging jobs")
        await _start_new_jobs(db, workload_manager, config)
    else:
        logger.info("updating merging jobs status")
        await _update_jobs(db, workload_manager, config)


async def merging_loop(
    db: AsyncDB,
    config: MergeConfig,
    workload_manager: WorkloadManager,
) -> None:
    logger.info("starting CrystFEL merging loop")
    counter = 0
    while True:
        await _merging_loop_iteration(db, workload_manager, config, counter % 30 != 0)
        counter += 1
