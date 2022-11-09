import asyncio
import re
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from time import time

import structlog
from structlog.stdlib import BoundLogger

from amarcord.amici.crystfel.util import make_cell_file_name
from amarcord.amici.crystfel.util import parse_cell_description
from amarcord.amici.crystfel.util import write_cell_file
from amarcord.amici.workload_manager.job_status import JobStatus
from amarcord.amici.workload_manager.workload_manager import JobStartError
from amarcord.amici.workload_manager.workload_manager import WorkloadManager
from amarcord.db.async_dbcontext import Connection
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.attributo_id import AttributoId
from amarcord.db.db_job_status import DBJobStatus
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.indexing_result import DBIndexingFOM
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
    cell_file_directory: Path
    indexing_script_path: Path
    chemical_attributo: AttributoId


async def write_cell_file_from_sample_in_db(
    db: AsyncDB,
    conn: Connection,
    run_id: int,
    base_path: Path,
    chemical_attributo: AttributoId,
    attributi: list[DBAttributo],
) -> None | str | Path:
    run = await db.retrieve_run(
        conn,
        run_id,
        attributi,
    )
    assert run is not None
    chemical_id = run.attributi.select_chemical_id(chemical_attributo)
    if chemical_id is None:
        return "chemical not set for run"
    chemical = await db.retrieve_chemical(conn, chemical_id, attributi)
    if chemical is None:
        return f"chemical {chemical_id} not found"
    cell_description_str = chemical.attributi.select_string(ATTRIBUTO_CELL_DESCRIPTION)
    if cell_description_str is None:
        return None
    if cell_description_str.strip() == "":
        return None
    cell_file = parse_cell_description(cell_description_str)
    if cell_file is None:
        return f"Cell description for chemical is wrong: {cell_description_str}"
    output_cell_file = base_path / f"chemical_{chemical_id}_{int(time())}.cell"
    try:
        write_cell_file(cell_file, output_cell_file)
    except Exception as e:
        return f"Cell file not writeable for chemical: {chemical_id}: {e}"
    return output_cell_file


async def start_indexing_job(
    workload_manager: WorkloadManager,
    config: CrystFELOnlineConfig,
    indexing_result: DBIndexingResultOutput,
) -> DBIndexingResultRuntimeStatus:
    logger.info(
        f"starting indexing job for run {indexing_result.run_id} and result id {indexing_result.id}"
    )

    job_base_directory = config.output_base_directory
    output_base_name = f"run_{indexing_result.run_id}_indexing_{indexing_result.id}"
    stream_file = job_base_directory / f"{output_base_name}.stream"
    try:
        if indexing_result.cell_description is not None:
            try:
                output_cell_file = config.cell_file_directory / make_cell_file_name(
                    indexing_result.cell_description
                )
                write_cell_file(indexing_result.cell_description, output_cell_file)
                logger.info(f"cell file written to {output_cell_file}")
            except Exception as e:
                return DBIndexingResultDone(
                    stream_file=stream_file,
                    job_error=f"Cell file not writeable for chemical: {e}",
                    fom=empty_indexing_fom,
                )
        else:
            output_cell_file = None

        logger.info(
            f'command line for this job is "{indexing_result.run_id}" "{stream_file}" "{output_cell_file}"'
        )
        job_start_result = await workload_manager.start_job(
            working_directory=job_base_directory,
            executable=config.indexing_script_path,
            command_line=f"{indexing_result.run_id} {stream_file} {output_cell_file}",
            time_limit=timedelta(days=1),
            stdout=job_base_directory / f"{output_base_name}_stdout.txt",
            stderr=job_base_directory / f"{output_base_name}_stderr.txt",
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


@dataclass(frozen=True)
class _IndexingFom:
    frames: int
    hits: int
    indexed_frames: int
    indexed_crystals: int


_INDEXING_RE = re.compile(
    r"(\d+) images processed, (\d+) hits \([^)]+\), (\d+) indexable \([^)]+\), (\d+) crystals, .*"
)


def calculate_indexing_fom_fast(
    this_logger: BoundLogger, stderr_file: Path
) -> None | _IndexingFom:
    if not stderr_file.is_file():
        return None
    with stderr_file.open("r") as f:
        images: None | int = None
        hits: None | int = None
        indexable: None | int = None
        crystals: None | int = None
        for line in f:
            match = _INDEXING_RE.search(line)
            if match is None:
                continue
            try:
                images = int(match.group(1))
                hits = int(match.group(2))
                indexable = int(match.group(3))
                crystals = int(match.group(4))
            except:
                this_logger.warning(f"indexing log line, but invalid format: {line}")
        if (
            images is not None
            and hits is not None
            and indexable is not None
            and crystals is not None
        ):
            return _IndexingFom(
                frames=images,
                hits=hits,
                indexed_frames=indexable,
                indexed_crystals=crystals,
            )
        return None


def _db_fom_from_raw_fom(fom: _IndexingFom) -> DBIndexingFOM:
    return DBIndexingFOM(
        hit_rate=fom.hits / fom.frames * 100.0 if fom.frames > 0 else 0.0,
        indexing_rate=fom.indexed_frames / fom.hits * 100.0 if fom.hits > 0 else 0.0,
        indexed_frames=fom.indexed_frames,
    )


async def _update_indexing_fom(
    log: BoundLogger,
    config: CrystFELOnlineConfig,
    indexing_result: DBIndexingResultOutput,
    runtime_status: DBIndexingResultRunning,
) -> None | DBIndexingResultRuntimeStatus:
    # Might not have been created by CrystFEL yet.
    if not Path(runtime_status.stream_file).is_file():
        log.info(
            f"cannot update figures of merit yet, missing {runtime_status.stream_file}"
        )
        return None

    log.info("calculating FoM")
    fom = calculate_indexing_fom_fast(
        log,
        config.output_base_directory
        / f"run_{indexing_result.run_id}_indexing_{indexing_result.id}_stderr.txt",
    )
    if fom is None:
        log.info("no FoM to be found")
        return None

    return DBIndexingResultRunning(
        stream_file=runtime_status.stream_file,
        job_id=runtime_status.job_id,
        fom=_db_fom_from_raw_fom(fom),
    )


async def _process_finished_indexing_job(
    log: BoundLogger,
    indexing_result_id: int,
    run_id: int,
    config: CrystFELOnlineConfig,
    runtime_status: DBIndexingResultRunning,
) -> DBIndexingResultRuntimeStatus:
    log.info("finished, calculating final FoM")
    fom = calculate_indexing_fom_fast(
        log,
        config.output_base_directory
        / f"run_{run_id}_indexing_{indexing_result_id}_stderr.txt",
    )
    if fom is None:
        log.info("final FoM calculation failed")
        return DBIndexingResultDone(
            stream_file=runtime_status.stream_file,
            job_error="Could not calculate final FoM",
            fom=runtime_status.fom,
        )

    log.info(f"final FoM calculation finished, writing to DB: {fom}")
    return DBIndexingResultDone(
        stream_file=runtime_status.stream_file,
        job_error=None,
        fom=_db_fom_from_raw_fom(fom),
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
            await asyncio.sleep(20)
    else:
        logger.info("no new indexing jobs to submit, take a break")
        await asyncio.sleep(1)


async def _update_jobs(
    db: AsyncDB, workload_manager: WorkloadManager, config: CrystFELOnlineConfig
) -> None:

    jobs_on_workload_manager = {j.id: j for j in await workload_manager.list_jobs()}

    async with db.read_only_connection() as conn:
        db_jobs = await db.retrieve_indexing_results(conn, DBJobStatus.RUNNING)

    for indexing_result in db_jobs:
        if isinstance(indexing_result.runtime_status, DBIndexingResultRunning):
            log = logger.bind(job_id=indexing_result.runtime_status.job_id)

            log.info(f"update job status, run id {indexing_result.run_id}")
            workload_job = jobs_on_workload_manager.get(
                indexing_result.runtime_status.job_id
            )
            # Job is still running, also on SLURM.
            if workload_job is not None and workload_job.status not in (
                JobStatus.FAILED,
                JobStatus.SUCCESSFUL,
            ):
                log.info(f"job for run id {indexing_result.run_id} still running")
                new_status = await _update_indexing_fom(
                    log, config, indexing_result, indexing_result.runtime_status
                )

            # Job has finished somehow (could be erroneous)
            else:
                log.info(f"job for run id {indexing_result.run_id} done")
                new_status = await _process_finished_indexing_job(
                    log,
                    indexing_result_id=indexing_result.id,
                    run_id=indexing_result.run_id,
                    config=config,
                    runtime_status=indexing_result.runtime_status,
                )

            async with db.begin() as conn:
                await db.update_indexing_result_status(
                    conn,
                    indexing_result_id=indexing_result.id,
                    runtime_status=new_status,
                )

    logger.info("indexing jobs stati updated, take a (longer) break")
    await asyncio.sleep(20)


async def _indexing_loop_iteration(
    db: AsyncDB,
    workload_manager: WorkloadManager,
    config: CrystFELOnlineConfig,
    start_new_jobs: bool,
) -> None:

    if start_new_jobs:
        logger.info("starting new indexing jobs")
        await _start_new_jobs(db, workload_manager, config)

    else:
        logger.info("updating indexing jobs status")
        await _update_jobs(db, workload_manager, config)


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
