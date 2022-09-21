import asyncio
import re
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from time import time

import structlog
from structlog.stdlib import BoundLogger

from amarcord.amici.workload_manager.job_status import JobStatus
from amarcord.amici.workload_manager.workload_manager import JobStartError
from amarcord.amici.workload_manager.workload_manager import WorkloadManager
from amarcord.db.async_dbcontext import Connection
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.attributo_id import AttributoId
from amarcord.db.indexing_result import DBIndexingFOM
from amarcord.db.indexing_result import DBIndexingResultDone
from amarcord.db.indexing_result import DBIndexingResultOutput
from amarcord.db.indexing_result import DBIndexingResultRunning
from amarcord.db.indexing_result import empty_indexing_fom

logger = structlog.stdlib.get_logger(__name__)

ATTRIBUTO_POINT_GROUP = AttributoId("point group")

ATTRIBUTO_CELL_DESCRIPTION = AttributoId("cell description")


@dataclass(frozen=True)
class CrystFELOnlineConfig:
    output_base_directory: Path
    cell_file_directory: Path
    indexing_script_path: Path
    sample_attributo: AttributoId


@dataclass(frozen=True, eq=True)
class CrystFELCellFile:
    lattice_type: str
    centering: str
    unique_axis: None | str

    a: float
    b: float
    c: float
    alpha: float
    beta: float
    gamma: float


_cell_description_regex = re.compile(
    r"(triclinic|monoclinic|orthorhombic|tetragonal|rhombohedral|hexagonal|cubic)\s+([PABCIFRH])\s+([abc?*])\s+\(([0-9]*(?:\.[0-9]*)?)\s+([0-9]*(?:\.[0-9]*)?)\s+([0-9]*(?:\.[0-9]*)?)\)\s+\(([0-9]*(?:\.[0-9]*)?)\s+([0-9]*(?:\.[0-9]*)?)\s+([0-9]*(?:\.[0-9]*)?)\)"
)


def parse_cell_description(s: str) -> None | CrystFELCellFile:
    match = _cell_description_regex.fullmatch(s)
    if match is None:
        return None
    unique_axis = match.group(3)
    return CrystFELCellFile(
        lattice_type=match.group(1),
        centering=match.group(2),
        unique_axis=None if unique_axis == "?" else unique_axis,
        a=float(match.group(4)),
        b=float(match.group(5)),
        c=float(match.group(6)),
        alpha=float(match.group(7)),
        beta=float(match.group(8)),
        gamma=float(match.group(9)),
    )


def write_cell_file(c: CrystFELCellFile, p: Path) -> None:
    with p.open("w") as f:
        f.write("CrystFEL unit cell file version 1.0\n\n")
        f.write(f"lattice_type = {c.lattice_type}\n")
        f.write(f"centering = {c.centering}\n")
        if c.unique_axis is not None:
            f.write(f"unique_axis = {c.unique_axis}\n\n")
        f.write(f"a = {c.a} A\n")
        f.write(f"b = {c.b} A\n")
        f.write(f"c = {c.c} A\n")
        f.write(f"al = {c.alpha} deg\n")
        f.write(f"be = {c.beta} deg\n")
        f.write(f"ga = {c.gamma} deg\n")


async def _retrieve_cell_file(
    db: AsyncDB,
    conn: Connection,
    run_id: int,
    base_path: Path,
    config: CrystFELOnlineConfig,
) -> None | str | Path:
    attributi = await db.retrieve_attributi(conn, associated_table=None)
    run = await db.retrieve_run(
        conn,
        run_id,
        attributi,
    )
    assert run is not None
    sample_id = run.attributi.select_sample_id(config.sample_attributo)
    if sample_id is None:
        return "Sample not set for run"
    sample = await db.retrieve_sample(conn, sample_id, attributi)
    if sample is None:
        return f"Sample {sample_id} not found"
    cell_description_str = sample.attributi.select_string(ATTRIBUTO_CELL_DESCRIPTION)
    if cell_description_str is None:
        return None
    cell_file = parse_cell_description(cell_description_str)
    if cell_file is None:
        return f"Cell description for sample is wrong: {cell_description_str}"
    output_cell_file = base_path / f"sample_{sample_id}_{int(time())}.cell"
    try:
        write_cell_file(cell_file, output_cell_file)
    except Exception as e:
        return f"Cell file not writeable for sample: {sample_id}: {e}"
    return output_cell_file


async def start_indexing_job(
    db: AsyncDB,
    workload_manager: WorkloadManager,
    config: CrystFELOnlineConfig,
    indexing_result: DBIndexingResultOutput,
) -> None:
    assert indexing_result.id is not None

    job_base_directory = config.output_base_directory
    output_base_name = f"run_{indexing_result.run_id}_indexing_{indexing_result.id}"
    stream_file = job_base_directory / f"{output_base_name}.stream"
    try:
        async with db.begin() as conn:
            output_cell_file = await _retrieve_cell_file(
                db, conn, indexing_result.run_id, config.cell_file_directory, config
            )
            if isinstance(output_cell_file, str):
                await db.update_indexing_result_status(
                    conn,
                    indexing_result_id=indexing_result.id,
                    runtime_status=DBIndexingResultDone(
                        stream_file=stream_file,
                        job_error=output_cell_file,
                        fom=empty_indexing_fom,
                    ),
                )
                return

        job_start_result = await workload_manager.start_job(
            working_directory=job_base_directory,
            executable=config.indexing_script_path,
            command_line=f"{stream_file} {output_cell_file}"
            if output_cell_file is not None
            else str(stream_file),
            time_limit=timedelta(days=1),
            stdout=job_base_directory / f"{output_base_name}_stdout.txt",
            stderr=job_base_directory / f"{output_base_name}_stderr.txt",
        )
        async with db.begin() as conn:
            await db.update_indexing_result_status(
                conn,
                indexing_result_id=indexing_result.id,
                runtime_status=DBIndexingResultRunning(
                    stream_file=stream_file,
                    job_id=job_start_result.job_id,
                    fom=empty_indexing_fom,
                ),
            )
    except JobStartError as e:
        async with db.begin() as conn:
            await db.update_indexing_result_status(
                conn,
                indexing_result_id=indexing_result.id,
                runtime_status=DBIndexingResultDone(
                    stream_file=stream_file,
                    job_error=e.message,
                    fom=empty_indexing_fom,
                ),
            )


async def _run_integer_tool(args: list[str]) -> None | int:
    assert args

    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    try:
        decoded_stdout = stdout.decode("utf-8").strip()
        # Special result value empty just means 0
        if decoded_stdout == "":
            return 0
        return int(decoded_stdout)
    except:
        logger.warning(
            f"couldn't get integer value from process {args}, stderr is {stderr!r}, stdout is {stdout!r}"
        )
        return None


@dataclass(frozen=True)
class _IndexingFom:
    frames: int
    hits: int
    indexed_frames: int
    indexed_crystals: int


async def _calculate_indexing_fom(stream_file: Path) -> None | _IndexingFom:
    frames = await _run_integer_tool(
        ["rg", "--count", "Image filename", str(stream_file)]
    )
    not_indexed_frames = await _run_integer_tool(
        [
            "rg",
            "--max-count",
            str(frames),
            "--count",
            "indexed_by = none",
            str(stream_file),
        ]
    )
    hits = await _run_integer_tool(
        ["rg", "--max-count", str(frames), "--count", "hit = 1", str(stream_file)]
    )
    indexed_crystals = await _run_integer_tool(
        ["rg", "--count", "Begin crystal", str(stream_file)]
    )
    return (
        _IndexingFom(
            frames=frames,
            hits=hits,
            indexed_frames=frames - not_indexed_frames,
            indexed_crystals=indexed_crystals,
        )
        if hits is not None
        and frames is not None
        and indexed_crystals is not None
        and not_indexed_frames is not None
        else None
    )


def _db_fom_from_raw_fom(fom: _IndexingFom) -> DBIndexingFOM:
    return DBIndexingFOM(
        hit_rate=fom.hits / fom.frames * 100.0 if fom.frames > 0 else 0.0,
        indexing_rate=fom.indexed_frames / fom.hits * 100.0 if fom.hits > 0 else 0.0,
        indexed_frames=fom.indexed_frames,
    )


async def _update_indexing_fom(
    log: BoundLogger,
    db: AsyncDB,
    indexing_result: DBIndexingResultOutput,
    runtime_status: DBIndexingResultRunning,
) -> None:
    # Might not have been created by CrystFEL yet.
    if not Path(runtime_status.stream_file).is_file():
        log.info(
            f"cannot update figures of merit yet, missing {runtime_status.stream_file}"
        )
        return

    log.info("calculating FoM")
    fom = await _calculate_indexing_fom(runtime_status.stream_file)
    if fom is None:
        log.info("no FoM to be found")
        return

    async with db.begin() as conn:
        log.info(f"FoM calculated, writing to DB: {fom}")
        await db.update_indexing_result_status(
            conn,
            indexing_result_id=indexing_result.id,
            runtime_status=DBIndexingResultRunning(
                stream_file=runtime_status.stream_file,
                job_id=runtime_status.job_id,
                fom=_db_fom_from_raw_fom(fom),
            ),
        )


async def _process_finished_indexing_job(
    log: BoundLogger,
    db: AsyncDB,
    indexing_result: DBIndexingResultOutput,
    runtime_status: DBIndexingResultRunning,
) -> None:
    log.info("finished, calculating final FoM")
    fom = await _calculate_indexing_fom(runtime_status.stream_file)
    if fom is None:
        log.info("final FoM calculation failed")
        async with db.begin() as conn:
            await db.update_indexing_result_status(
                conn,
                indexing_result_id=indexing_result.id,
                runtime_status=DBIndexingResultDone(
                    stream_file=runtime_status.stream_file,
                    job_error="Could not calculate final FoM",
                    fom=runtime_status.fom,
                ),
            )
        return

    async with db.begin() as conn:
        log.info(f"final FoM calculation finished, writing to DB: {fom}")
        await db.update_indexing_result_status(
            conn,
            indexing_result_id=indexing_result.id,
            runtime_status=DBIndexingResultDone(
                stream_file=runtime_status.stream_file,
                job_error=None,
                fom=_db_fom_from_raw_fom(fom),
            ),
        )


async def _indexing_loop_iteration(
    db: AsyncDB, workload_manager: WorkloadManager, config: CrystFELOnlineConfig
) -> None:
    async with db.read_only_connection() as conn:
        indexing_results = await db.retrieve_indexing_results(conn)

    jobs_on_workload_manager = {j.id: j for j in await workload_manager.list_jobs()}
    for indexing_result in indexing_results:
        if indexing_result.runtime_status is None:
            await start_indexing_job(db, workload_manager, config, indexing_result)
        elif isinstance(indexing_result.runtime_status, DBIndexingResultRunning):
            log = logger.bind(job_id=indexing_result.runtime_status.job_id)
            # This is reserved for Om indexing
            if indexing_result.runtime_status.job_id == 0:
                continue
            workload_job = jobs_on_workload_manager.get(
                indexing_result.runtime_status.job_id
            )
            # Job is still running, also on SLURM.
            if workload_job is not None and workload_job.status not in (
                JobStatus.FAILED,
                JobStatus.SUCCESSFUL,
            ):
                await _update_indexing_fom(
                    log, db, indexing_result, indexing_result.runtime_status
                )
            else:
                # Job has finished somehow (could be erroneous)
                await _process_finished_indexing_job(
                    log, db, indexing_result, indexing_result.runtime_status
                )
        else:
            # Nothing to do for jobs that are done (we could filter them via SQL beforehand if it's too much to handle).
            assert isinstance(indexing_result.runtime_status, DBIndexingResultDone)


async def indexing_loop(
    db: AsyncDB,
    workload_manager: WorkloadManager,
    config: CrystFELOnlineConfig,
    sleep_seconds: float,
) -> None:
    while True:
        async with db.read_only_connection() as conn:
            user_config = await db.retrieve_configuration(conn)

        if user_config.use_online_crystfel:
            await _indexing_loop_iteration(db, workload_manager, config)

        await asyncio.sleep(sleep_seconds)
