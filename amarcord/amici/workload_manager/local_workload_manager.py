import asyncio
import datetime
import logging
import shutil
import stat
from asyncio.subprocess import Process
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from amarcord.amici.workload_manager.job import Job
from amarcord.amici.workload_manager.job import JobMetadata
from amarcord.amici.workload_manager.job_status import JobStatus
from amarcord.amici.workload_manager.workload_manager import JobStartResult
from amarcord.amici.workload_manager.workload_manager import WorkloadManager

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WrappedProcess:
    process: Process
    started: datetime.datetime


async def start_process_locally(
    output_base_dir_str: str,
    script: str,
    extra_file_paths_str: list[str],
) -> tuple[Process, Path]:
    output_base_dir = Path(output_base_dir_str)

    script_path = output_base_dir / "script"
    with script_path.open("w", encoding="utf-8") as f:
        f.write(script)

    script_path.chmod(script_path.stat().st_mode | stat.S_IEXEC)

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

    proc = await asyncio.create_subprocess_shell(
        str(script_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    return proc, process_dir


class LocalWorkloadManager(WorkloadManager):
    # pylint: disable=super-init-not-called
    def __init__(self) -> None:
        self._processes: list[WrappedProcess] = []

    async def start_job(
        self,
        working_directory: Path,
        script: str,
        name: str,
        time_limit: datetime.timedelta,
        stdout: None | Path = None,
        stderr: None | Path = None,
    ) -> JobStartResult:
        process, _ = await start_process_locally(
            str(working_directory),
            script,
            [],
        )
        self._processes.append(
            WrappedProcess(process, datetime.datetime.now(datetime.timezone.utc))
        )
        return JobStartResult(
            job_id=process.pid, metadata=JobMetadata({"pid": process.pid})
        )

    async def list_jobs(self) -> Iterable[Job]:
        result: list[Job] = []
        for wrapped_process in self._processes:
            rc = wrapped_process.process.returncode
            result.append(
                Job(
                    id=wrapped_process.process.pid,
                    status=(
                        JobStatus.SUCCESSFUL
                        if rc is not None and rc == 0
                        else JobStatus.RUNNING if rc is None else JobStatus.FAILED
                    ),
                    started=wrapped_process.started,
                    metadata=JobMetadata({"pid": wrapped_process.process.pid}),
                )
            )
        return result
