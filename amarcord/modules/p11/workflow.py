import asyncio
import datetime
import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from typing import Generator
from typing import Optional

from amarcord.db.db import DB
from amarcord.db.indexing_job_status import IndexingJobStatus
from amarcord.db.table_classes import DBIndexingJob
from amarcord.db.table_classes import DBIndexingParameter
from amarcord.modules.dbcontext import Connection
from amarcord.modules.p11.job import Job
from amarcord.modules.p11.job_status import JobStatus
from amarcord.modules.p11.slurm_rest_job_controller import SlurmRestJobController
from amarcord.util import read_file_to_string
from amarcord.util import sha256_files
from amarcord.util import str_to_int

logger = logging.getLogger(__name__)


def extract_geom_from_crystfel_project_file(fp: Path) -> Optional[Path]:
    with fp.open("r") as f:
        for line in f:
            if not line.startswith("geom "):
                continue

            path = line[5:]

            if path:
                return Path(path)

    return None


async def project_file_daemon_iteration(
    db: DB, project_file_location: Path, previous_hash: Optional[str]
) -> Optional[str]:
    if not project_file_location.exists():
        return previous_hash

    geom_file = extract_geom_from_crystfel_project_file(project_file_location)

    if geom_file is None:
        logger.info(
            "couldn't find geometry file in %s, not using project file",
            project_file_location,
        )
        return previous_hash

    total_hash = sha256_files([project_file_location, geom_file])

    if total_hash == previous_hash:
        return previous_hash

    now = datetime.datetime.utcnow()
    with db.connect() as conn:
        db.add_indexing_parameter(
            conn,
            DBIndexingParameter(
                id=0,
                project_file_first_discovery=now,
                project_file_last_discovery=now,
                project_file_path=project_file_location,
                project_file_content=read_file_to_string(project_file_location),
                geometry_file_content=read_file_to_string(geom_file),
                project_file_hash=total_hash,
            ),
        )

    return total_hash


async def project_file_daemon(
    db: DB, project_file_location: Path, wait_time_seconds: float = 1.0
) -> None:
    previous_hash: Optional[str] = None

    while True:
        await asyncio.sleep(wait_time_seconds)

        previous_hash = await project_file_daemon_iteration(
            db, project_file_location, previous_hash
        )


@dataclass(frozen=True, eq=True)
class P11MasterFile:
    path: Path
    run_id: int


def path_to_master_file(path: Path) -> Optional[P11MasterFile]:
    dir_components = path.parent.name.split("_")

    if len(dir_components) != 2:
        return None

    run_id = str_to_int(dir_components[1])

    if run_id is None:
        return None

    return P11MasterFile(path=path, run_id=run_id)


def list_p11_master_files(
    base_path: Path,
) -> Generator[Optional[P11MasterFile], None, None]:
    return (path_to_master_file(p) for p in base_path.glob("raw/*/*_master.h5"))


@dataclass(frozen=True)
class CommandlinerParameters:
    project_file: Path
    files_path: Path
    output_streamfile: Path


@dataclass(frozen=True)
class IndexingJobStartResult:
    job_id: int
    output_directory: Path
    command_line: str


Commandliner = Callable[[CommandlinerParameters], str]


def standard_commandliner(
    singularity_file: Optional[Path], params: CommandlinerParameters
) -> str:
    args = [
        "commandliner",
        "--project-file",
        str(params.project_file),
        "--input",
        str(params.files_path),
        "--output-stream",
        str(params.output_streamfile),
    ]
    if singularity_file is not None:
        args.extend(
            [
                "--indexamajig-path" f"singularity exec {singularity_file} indexamajig",
            ]
        )
    output = subprocess.run(args, check=True, capture_output=True)
    return output.stdout.decode("utf-8")


async def start_indexing_job(
    job_controller: SlurmRestJobController,
    commandliner: Commandliner,
    project_file: Path,
    master_file: P11MasterFile,
) -> IndexingJobStartResult:
    relative_sub_path = Path(str(master_file.run_id))

    crystfel_command_line = commandliner(
        CommandlinerParameters(
            project_file,
            files_path=relative_sub_path / "files.lst",
            output_streamfile=relative_sub_path / "output.stream",
        )
    )
    command_line = f"echo '{master_file.path}' > files.lst;\n{crystfel_command_line}"

    result = job_controller.start_job(
        relative_sub_path=relative_sub_path,
        command_line=command_line,
        extra_files=[],
    )

    return IndexingJobStartResult(
        result.job_id, result.output_directory, crystfel_command_line
    )


async def process_run_master_file(
    db: DB,
    conn: Connection,
    job_controller: SlurmRestJobController,
    commandliner: Commandliner,
    current_indexing_parameter: DBIndexingParameter,
    master_file: P11MasterFile,
) -> None:
    if db.run_has_indexing_jobs(
        conn,
        run_id=master_file.run_id,
        indexing_parameter_id=current_indexing_parameter.id,
    ):
        return

    start_job_result = await start_indexing_job(
        job_controller,
        commandliner,
        current_indexing_parameter.project_file_path,
        master_file,
    )

    db.add_indexing_job(
        conn,
        DBIndexingJob(
            id=0,
            started=datetime.datetime.utcnow(),
            stopped=None,
            output_directory=start_job_result.output_directory,
            run_id=master_file.run_id,
            indexing_parameter_id=current_indexing_parameter.id,
            master_file=master_file.path,
            command_line=start_job_result.command_line,
            status=IndexingJobStatus.RUNNING,
            slurm_job_id=start_job_result.job_id,
            result_file=None,
            error_message=None,
        ),
    )


MasterFileProvider = Callable[[], Generator[Optional[P11MasterFile], None, None]]


async def run_observing_daemon(
    db: DB,
    commandliner: Commandliner,
    master_file_provider: MasterFileProvider,
    job_controller: SlurmRestJobController,
    wait_time_seconds: float = 5,
) -> None:
    last_master_files = set(r for r in master_file_provider() if r is not None)

    # When starting the daemon, we might have missed some master files (the daemon might have crashed or started late)
    with db.connect() as conn:
        indexing_parameter = db.retrieve_latest_indexing_parameter(conn)
        if indexing_parameter is not None:
            for master_file in last_master_files:
                await process_run_master_file(
                    db,
                    conn,
                    job_controller,
                    commandliner,
                    indexing_parameter,
                    master_file,
                )

    while True:
        await asyncio.sleep(wait_time_seconds)

        current_master_files = set(r for r in master_file_provider() if r is not None)

        new_master_files = current_master_files - last_master_files

        with db.connect() as conn:
            indexing_parameter = db.retrieve_latest_indexing_parameter(conn)

            if indexing_parameter is None:
                continue

            for master_file in new_master_files:
                await process_run_master_file(
                    db,
                    conn,
                    job_controller,
                    commandliner,
                    indexing_parameter,
                    master_file,
                )

            await process_finished_jobs(conn, db, job_controller)


async def process_finished_jobs(conn, db, job_controller):
    running_jobs_on_slurm = {j.job_id: j for j in job_controller.list_jobs()}
    for job_in_db in db.retrieve_indexing_jobs(conn, only_running=True):
        job_on_slurm: Optional[Job] = running_jobs_on_slurm.get(
            job_in_db.slurm_job_id, None
        )

        if job_on_slurm is None or job_on_slurm.status in (
            JobStatus.RUNNING,
            JobStatus.QUEUED,
        ):
            continue

        output_stream_file = job_in_db.output_directory / "output.stream"

        if not output_stream_file.is_file():
            db.finish_indexing_job_error(
                conn, job_in_db.id, "couldn't find output.stream"
            )
        else:
            db.finish_indexing_job_successfully(conn, job_in_db.id, output_stream_file)
