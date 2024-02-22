import asyncio
import datetime
import inspect
import json
import shlex
from base64 import b64encode
from dataclasses import dataclass
from datetime import timedelta
from enum import IntEnum
from enum import auto
from io import StringIO
from pathlib import Path

import structlog
from pydantic import BaseModel
from structlog.stdlib import BoundLogger

import amarcord.cli.crystfel_merge
from amarcord.amici.crystfel.util import coparse_cell_file
from amarcord.amici.crystfel.util import determine_output_directory
from amarcord.amici.crystfel.util import make_cell_file_name
from amarcord.amici.workload_manager.job_status import JobStatus
from amarcord.amici.workload_manager.workload_manager import JobStartError
from amarcord.amici.workload_manager.workload_manager import WorkloadManager
from amarcord.db.async_dbcontext import Connection
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.db_job_status import DBJobStatus
from amarcord.db.db_merge_result import DBMergeResultOutput
from amarcord.db.db_merge_result import DBMergeRuntimeStatus
from amarcord.db.db_merge_result import DBMergeRuntimeStatusError
from amarcord.db.db_merge_result import DBMergeRuntimeStatusRunning
from amarcord.db.indexing_result import DBIndexingResultDone
from amarcord.db.merge_parameters import DBMergeParameters
from amarcord.db.merge_result import MergeResult
from amarcord.db.run_internal_id import RunInternalId
from amarcord.db.scale_intensities import ScaleIntensities
from amarcord.db.table_classes import DBFile

logger = structlog.stdlib.get_logger(__name__)

_SHORT_SLEEP_DURATION_SECONDS = 2.0
_LONG_SLEEP_DURATION_SECONDS = 20.0


@dataclass(frozen=True)
class MergeConfig:
    output_base_directory: Path
    crystfel_path: Path
    api_url: str


def _find_file_id_by_extension(files: list[DBFile], extension: str) -> None | int:
    for f in files:
        if f.file_name.endswith(f".{extension}"):
            return f.id
    return None


def merge_parameters_to_crystfel_parameters(p: DBMergeParameters) -> list[str]:
    result = [
        f"--symmetry={p.point_group}",
        f"--iterations={p.iterations}",
        f"--max-rel-B={p.rel_b}",
    ]
    if p.no_pr:
        result.append("--no-pr")
    if p.force_bandwidth is not None:
        result.append(f"--force-bandwidth={p.force_bandwidth}")
    if p.force_radius is not None:
        result.append(f"--force-radius={p.force_radius}")
    if p.force_lambda is not None:
        result.append(f"--force-lambda={p.force_lambda}")
    match p.scale_intensities:
        case ScaleIntensities.OFF:
            result.append("--no-scale")
        case ScaleIntensities.NORMAL:
            result.append("--no-Bscale")
        case ScaleIntensities.DEBYE_WALLER:
            pass
    if p.no_delta_cc_half:
        result.append("--no-deltacchalf")
    if p.start_after is not None:
        result.append(f"--start-after={p.start_after}")
    if p.stop_after is not None:
        result.append(f"--stop-after={p.stop_after}")
    result.append(f"--model={p.merge_model.value}")
    if p.polarisation is not None:
        result.append(
            f"--polarisation={p.polarisation.angle.m}deg{p.polarisation.percentage}"
        )
    if p.max_adu is not None:
        result.append(f"--max-adu={p.max_adu}")
    if p.min_res is not None:
        result.append(f"--min-res={p.min_res}")
    result.append(f"--min-measurements={p.min_measurements}")
    if p.push_res is not None:
        result.append(f"--push-res={p.push_res}")
    if not p.logs:
        result.append("--no-logs")
    if p.w is not None:
        result.extend(["-w", p.w])
    return result


async def start_merge_job(
    db: AsyncDB,
    conn: Connection,
    parent_logger: BoundLogger,
    workload_manager: WorkloadManager,
    config: MergeConfig,
    merge_result: DBMergeResultOutput,
) -> DBMergeRuntimeStatus:
    parent_logger.info(
        f"starting merge job, indexing results {[ir.id for ir in merge_result.indexing_results]}, parameters {merge_result.parameters}"
    )

    finished_results: list[DBIndexingResultDone] = []
    pdb_file_id: None | int = None
    restraints_cif_file_id: None | int = None
    attributi = await db.retrieve_attributi(
        conn, beamtime_id=None, associated_table=None
    )
    random_run_id: None | RunInternalId = None
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
        random_run_id = ir.run_id
        this_pdb_file_id = _find_file_id_by_extension(chemical.files, "pdb")
        if this_pdb_file_id is not None:
            pdb_file_id = this_pdb_file_id
        this_restraints_cif_file_id = _find_file_id_by_extension(chemical.files, "cif")
        if this_restraints_cif_file_id is not None:
            restraints_cif_file_id = this_restraints_cif_file_id
        finished_results.append(ir.runtime_status)
    parent_logger.info("All indexing results have finished, we can start merging")

    assert (
        random_run_id is not None
    ), "one of the indexing results doesn't have a run ID, how can that be?"
    random_run = await db.retrieve_run(conn, random_run_id, attributi)

    cell_file_contents = StringIO()
    coparse_cell_file(merge_result.parameters.cell_description, cell_file_contents)
    cell_file_id = (
        await db.create_file_from_bytes(
            conn,
            file_name=make_cell_file_name(merge_result.parameters.cell_description),
            description="",
            original_path=Path("/tmp/cell-file.cell"),
            contents=cell_file_contents.getvalue().encode("utf-8"),
            deduplicate=True,
        )
    ).id

    try:
        with Path(inspect.getfile(amarcord.cli.crystfel_merge)).open(
            "r", encoding="utf-8"
        ) as merge_file:
            predefined_args = {
                "stream-files": [str(ir.stream_file) for ir in finished_results],
                "api-url": config.api_url,
                "merge-result-id": merge_result.id,
                "cell-file-id": cell_file_id,
                "point-group": merge_result.parameters.point_group,
                "partialator-additional": shlex.join(
                    merge_parameters_to_crystfel_parameters(merge_result.parameters)
                ),
                "crystfel-path": str(config.crystfel_path),
                "pdb-file-id": pdb_file_id,
                "restraints-cif-file-id": restraints_cif_file_id,
            }
            parent_logger.info(
                "command line for this job is "
                + " ".join(f"{k}={v}" for k, v in predefined_args.items())
            )
            predefined_args_b64 = b64encode(
                json.dumps(predefined_args, allow_nan=False).encode("utf-8")
            ).decode("utf-8")
            merge_file_contents = merge_file.read().replace(
                "predefined_args: None | bytes = None",
                f'predefined_args = "{predefined_args_b64}"',
            )
            beamtime = await db.retrieve_beamtime(conn, random_run.beamtime_id)
            job_base_directory = determine_output_directory(
                beamtime,
                config.output_base_directory,
                {},
            )
            job_start_result = await workload_manager.start_job(
                working_directory=job_base_directory,
                name=f"mg_{merge_result.id}",
                script=merge_file_contents,
                time_limit=timedelta(days=1),
                stdout=job_base_directory / f"{merge_result.id}_stdout.txt",
                stderr=job_base_directory / f"{merge_result.id}_stderr.txt",
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


class JsonMergeResultRootJson(BaseModel):
    error: None | str
    result: None | MergeResult


def _process_premature_finish(
    log: BoundLogger,
    runtime_status: DBMergeRuntimeStatusRunning,
) -> DBMergeRuntimeStatusError:
    log.info("job has finished prematurely")

    return DBMergeRuntimeStatusError(
        error="job has finished on SLURM, but delivered no results",
        started=runtime_status.started,
        stopped=datetime.datetime.utcnow(),
        recent_log=runtime_status.recent_log,
    )


class CheckResult(IntEnum):
    CHECK_ACTION = auto()
    CHECK_NO_ACTION = auto()


async def _start_new_jobs(
    db: AsyncDB, workload_manager: WorkloadManager, config: MergeConfig
) -> CheckResult:
    async with db.read_only_connection() as conn:
        merge_results = await db.retrieve_merge_results(conn, DBJobStatus.QUEUED)

    if not merge_results:
        return CheckResult.CHECK_NO_ACTION

    for merge_result in merge_results:
        result_logger = logger.bind(merge_result_id=merge_result.id)
        result_logger.info("starting merge job")

        async with db.begin() as conn:
            new_status = await start_merge_job(
                db, conn, result_logger, workload_manager, config, merge_result
            )
            await db.update_merge_result_status(conn, merge_result.id, new_status)

        logger.info("new merge job submitted, taking a long break")
        await asyncio.sleep(_LONG_SLEEP_DURATION_SECONDS)

    return CheckResult.CHECK_ACTION


async def _update_jobs(db: AsyncDB, workload_manager: WorkloadManager) -> CheckResult:
    async with db.read_only_connection() as conn:
        db_jobs = await db.retrieve_merge_results(conn, DBJobStatus.RUNNING)

    if not db_jobs:
        return CheckResult.CHECK_NO_ACTION

    jobs_on_workload_manager = {j.id: j for j in await workload_manager.list_jobs()}
    await asyncio.sleep(20)

    for merge_result in db_jobs:
        result_logger = logger.bind(merge_result_id=merge_result.id)

        if not isinstance(merge_result.runtime_status, DBMergeRuntimeStatusRunning):
            continue

        job_logger = result_logger.bind(job_id=merge_result.runtime_status.job_id)
        workload_job = jobs_on_workload_manager.get(merge_result.runtime_status.job_id)
        if workload_job is not None and workload_job.status not in (
            JobStatus.FAILED,
            JobStatus.SUCCESSFUL,
        ):
            # Running job, let it keep running
            continue

        if workload_job is None:
            result_logger.info("finished because not in SLURM REST job list anymore")
        else:
            result_logger.info(
                f"finished because SLURM REST job status is {workload_job.status}"
            )

        # Job has finished prematurely
        new_status = _process_premature_finish(job_logger, merge_result.runtime_status)

        async with db.begin() as conn:
            await db.update_merge_result_status(conn, merge_result.id, new_status)

    return CheckResult.CHECK_ACTION


async def _merging_loop_iteration(
    db: AsyncDB,
    workload_manager: WorkloadManager,
    config: MergeConfig,
) -> None:
    check_result_start = await _start_new_jobs(db, workload_manager, config)
    check_result_update = await _update_jobs(db, workload_manager)

    # If both actions did nothing, we sleep for a bit to not overload the database with queries
    # If one of the actions did something, we have our long sleep for the REST interface anyhow.
    if (
        check_result_start == CheckResult.CHECK_NO_ACTION
        and check_result_update == CheckResult.CHECK_NO_ACTION
    ):
        await asyncio.sleep(_SHORT_SLEEP_DURATION_SECONDS)


async def merging_loop(
    db: AsyncDB,
    config: MergeConfig,
    workload_manager: WorkloadManager,
) -> None:
    logger.info("starting CrystFEL merging loop")
    while True:
        await _merging_loop_iteration(db, workload_manager, config)
