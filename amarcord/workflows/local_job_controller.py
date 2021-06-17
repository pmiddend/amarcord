import datetime
import logging
import shutil
import subprocess
from multiprocessing import Process
from pathlib import Path
from time import time
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple
from typing import TypedDict
from typing import cast

import sqlalchemy as sa

from amarcord.modules.dbcontext import CreationMode
from amarcord.modules.dbcontext import DBContext
from amarcord.modules.json import JSONDict
from amarcord.workflows.job import Job
from amarcord.workflows.job_controller import JobController
from amarcord.workflows.job_controller import JobStartResult
from amarcord.workflows.job_status import JobStatus

logger = logging.getLogger(__name__)

try:
    import psutil
    from psutil import NoSuchProcess
except:
    logger.info("cannot import psutil, local job controller not usable")


class JobResult(TypedDict):
    failed: bool
    reason: Optional[str]


def _command_subprocess(subprocess_command: str, process_dir_str: str) -> None:
    process_dir = Path(process_dir_str)

    stdout_file = (process_dir / "stdout.txt").open("w")
    stderr_file = (process_dir / "stderr.txt").open("w")

    with subprocess.Popen(
        subprocess_command,
        shell=True,
        cwd=process_dir,
        stdout=stdout_file,
        stderr=stderr_file,
    ) as waiting_process:
        logger.info("subprocess wait...")

        waiting_process.wait()

        logger.info("subprocess done: %s...", waiting_process.returncode)


def start_process_locally(
    output_base_dir_str: str,
    executable_path_str: str,
    command_line: str,
    extra_file_paths_str: List[str],
) -> Tuple[int, Path]:
    output_base_dir = Path(output_base_dir_str)
    executable_path = Path(executable_path_str)
    extra_file_paths = [Path(s) for s in extra_file_paths_str]

    process_dir = output_base_dir / str(int(time()))
    process_dir.mkdir(parents=True)

    for extra_file in extra_file_paths:
        logger.info(
            "Copying extra file %s to output directory as %s",
            extra_file,
            Path(extra_file).name,
        )
        shutil.copyfile(extra_file, process_dir / extra_file.name)

    logger.info(
        "Copying main executable %s to output directory as %s",
        executable_path,
        executable_path.name,
    )
    relative_executable = process_dir / executable_path.name
    # Important to use copy2 here because it copies file permissions as well:
    # https://stackoverflow.com/questions/123198/how-can-a-file-be-copied
    shutil.copy2(executable_path, relative_executable)

    subprocess_command = f"{relative_executable} {command_line}"
    logger.info("Running the following command line: %s", subprocess_command)

    p = Process(target=_command_subprocess, args=(subprocess_command, str(process_dir)))
    p.start()

    return cast(int, p.pid), process_dir


class LocalJobController(JobController):
    def __init__(self, job_db_file: Path, output_base_dir: Path) -> None:
        super().__init__()
        self._output_base_dir = output_base_dir
        db_file_ = f"sqlite:///{job_db_file}"
        self._db = DBContext(db_file_)
        self._processes_table = sa.Table(
            "Processes",
            self._db.metadata,
            sa.Column("process_id", sa.Integer(), primary_key=True),
            sa.Column(
                "started", sa.DateTime(), server_default=sa.func.now(), nullable=False
            ),
        )
        self._db.create_all(CreationMode.CHECK_FIRST)

    def start_job(
        self,
        relative_sub_path: Path,
        executable: Path,
        command_line: str,
        extra_files: List[Path],
    ) -> JobStartResult:
        task = start_process_locally(
            str(self._output_base_dir / relative_sub_path),
            str(executable),
            command_line,
            [str(s) for s in extra_files],
        )
        with self._db.connect() as conn:
            conn.execute(
                sa.insert(self._processes_table).values(process_id=str(task[0]))
            )
            return JobStartResult(metadata={"pid": task[0]}, output_directory=task[1])

    def list_jobs(self) -> Iterable[Job]:
        with self._db.connect() as conn:
            completed: List[int] = []
            for process in conn.execute(
                sa.select(
                    [
                        self._processes_table.c.process_id,
                        self._processes_table.c.started,
                    ]
                )
            ).fetchall():
                pid_int, started = process[0], process[1]
                try:
                    process = psutil.Process(pid_int)
                    metadata = {
                        "pid": pid_int,
                        "realStatus": process.status(),
                        "cmdline": process.cmdline(),
                        "cwd": process.cwd(),
                    }
                    if process.status() == "zombie":
                        yield Job(JobStatus.COMPLETED, started, metadata)
                    else:
                        yield Job(JobStatus.RUNNING, started, metadata)
                except NoSuchProcess:
                    completed.append(pid_int)
                    yield Job(JobStatus.COMPLETED, started, {"pid": pid_int})

            conn.execute(
                sa.delete(self._processes_table).where(
                    sa.and_(
                        self._processes_table.c.process_id.in_(completed),
                        self._processes_table.c.started
                        < (datetime.datetime.now() - datetime.timedelta(minutes=5)),
                    )
                )
            )

    def equals(self, metadata_a: JSONDict, metadata_b: JSONDict) -> bool:
        return metadata_a.get("pid", None) == metadata_b.get("pid", None)
