import asyncio
import datetime
import shutil
import subprocess
from asyncio.subprocess import Process
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from typing import TypedDict

import structlog

from amarcord.amici.slurm.job import Job
from amarcord.amici.slurm.job import JobMetadata
from amarcord.amici.slurm.job_controller import JobController
from amarcord.amici.slurm.job_controller import JobStartResult
from amarcord.amici.slurm.job_status import JobStatus
from amarcord.json_types import JSONDict

logger = structlog.stdlib.get_logger(__name__)


class JobResult(TypedDict):
    failed: bool
    reason: str


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


@dataclass(frozen=True)
class WrappedProcess:
    process: Process
    started: datetime.datetime


async def start_process_locally(
    output_base_dir_str: str,
    executable_path_str: str,
    command_line: str,
    extra_file_paths_str: list[str],
) -> tuple[Process, Path]:
    output_base_dir = Path(output_base_dir_str)
    executable_path = Path(executable_path_str)
    extra_file_paths = [Path(s) for s in extra_file_paths_str]

    process_dir = output_base_dir
    process_dir.mkdir(parents=True, exist_ok=True)

    for extra_file in extra_file_paths:
        logger.info(
            "Copying extra file %s to output directory as %s",
            extra_file,
            Path(extra_file).name,
        )
        shutil.copyfile(extra_file, process_dir / extra_file.name)

    logger.info(
        f"Copying main executable {executable_path} to output directory as {executable_path.name}"
    )
    relative_executable = process_dir / executable_path.name
    # Important to use copy2 here because it copies file permissions as well:
    # https://stackoverflow.com/questions/123198/how-can-a-file-be-copied
    shutil.copy2(executable_path, relative_executable)

    subprocess_command = f"{relative_executable} {command_line}"
    logger.info(f"Running the following command line: {subprocess_command}")

    proc = await asyncio.create_subprocess_shell(
        subprocess_command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    return proc, process_dir


class LocalJobController(JobController):
    # pylint: disable=super-init-not-called
    def __init__(self) -> None:
        self._processes: list[WrappedProcess] = []

    async def start_job(
        self,
        path: Path,
        executable: Path,
        command_line: str,
        time_limit: datetime.timedelta,
        extra_files: list[Path],
    ) -> JobStartResult:
        process, out_dir = await start_process_locally(
            str(path),
            str(executable),
            command_line,
            [str(s) for s in extra_files],
        )
        self._processes.append(WrappedProcess(process, datetime.datetime.utcnow()))
        return JobStartResult(
            metadata=JobMetadata({"pid": process.pid}), output_directory=out_dir
        )

    async def list_jobs(self) -> Iterable[Job]:
        result: list[Job] = []
        for wrapped_process in self._processes:
            rc = wrapped_process.process.returncode
            result.append(
                Job(
                    status=JobStatus.COMPLETED
                    if rc is not None and rc == 0
                    else JobStatus.RUNNING
                    if rc is None
                    else JobStatus.FAILED,
                    started=wrapped_process.started,
                    metadata=JobMetadata({"pid": wrapped_process.process.pid}),
                )
            )
        return result

    def equals(self, metadata_a: JSONDict, metadata_b: JSONDict) -> bool:
        return metadata_a.get("pid", None) == metadata_b.get("pid", None)

    def is_our_job(self, metadata_a: JSONDict) -> bool:
        return True

    def should_restart(self, job_id: int, output_directory: Path) -> bool:
        return False
