import asyncio
import datetime
import logging
import os
import shutil
import stat
from asyncio.subprocess import Process
from dataclasses import dataclass
from time import time
from typing import Any
from typing import Iterable
from typing import override

from anyio import Path

from amarcord.amici.workload_manager.job import Job
from amarcord.amici.workload_manager.job_status import JobStatus
from amarcord.amici.workload_manager.workload_manager import JobStartError
from amarcord.amici.workload_manager.workload_manager import JobStartResult
from amarcord.amici.workload_manager.workload_manager import WorkloadManager

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WrappedProcess:
    process: Process
    started: datetime.datetime
    script_path: Path


async def start_process_locally(
    output_base_dir_str: str,
    script: str,
    environment: dict[str, str],
    extra_file_paths_str: list[str],
    stdout: None | Path = None,
    stderr: None | Path = None,
) -> tuple[Process, Path, Path]:
    output_base_dir = Path(output_base_dir_str)

    script_path = output_base_dir / f"{time()}-amarcord-script.sh"
    async with await script_path.open("w", encoding="utf-8") as f:
        await f.write(script)

    await script_path.chmod((await script_path.stat()).st_mode | stat.S_IEXEC)

    extra_file_paths = [Path(s) for s in extra_file_paths_str]

    process_dir = output_base_dir
    await process_dir.mkdir(parents=True, exist_ok=True)

    for extra_file in extra_file_paths:
        logger.info(
            "Copying extra file %s to output directory as %s",
            extra_file,
            Path(extra_file).name,
        )
        shutil.copyfile(extra_file, process_dir / extra_file.name)

    # Without this, we wouldn't even have PATH, which constrains usefulness of this workload manager
    sub_environ = os.environ.copy()
    sub_environ.update(environment)

    async def create_subprocess(stdout: Any, stderr: Any) -> Any:
        return await asyncio.create_subprocess_shell(
            str(script_path),
            stdout=stdout,
            stderr=stderr,
            env=sub_environ,
            cwd=output_base_dir_str,
        )

    if stdout is not None:
        if stderr is not None:
            async with (
                await stdout.open("w") as stdout_obj,
                await stderr.open("w") as stderr_obj,
            ):
                proc = await create_subprocess(
                    stdout=stdout_obj,
                    stderr=stderr_obj,
                )
        else:
            async with await stdout.open("w") as stdout_obj:
                proc = await create_subprocess(
                    stdout=stdout_obj,
                    stderr=asyncio.subprocess.PIPE,
                )
    elif stderr is not None:
        async with await stderr.open("w") as stderr_obj:
            proc = await create_subprocess(
                stdout=asyncio.subprocess.PIPE,
                stderr=stderr_obj,
            )
    else:
        proc = await create_subprocess(
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

    return proc, process_dir, script_path


class LocalWorkloadManager(WorkloadManager):
    def __init__(self) -> None:
        self._processes: list[WrappedProcess] = []

    @override
    def name(self) -> str:
        return "local processes"

    @override
    async def start_job(
        self,
        working_directory: Path,
        script: str,
        name: str,
        time_limit: datetime.timedelta,
        environment: dict[str, str],
        stdout: None | Path = None,
        stderr: None | Path = None,
    ) -> JobStartResult:
        try:
            await working_directory.mkdir(exist_ok=True, parents=True)
        except:
            raise JobStartError(
                f"couldn't create working directory {working_directory}"
            )
        process, _, script_path = await start_process_locally(
            output_base_dir_str=str(working_directory),
            script=script,
            environment=environment,
            extra_file_paths_str=[],
            stdout=stdout,
            stderr=stderr,
        )
        self._processes.append(
            WrappedProcess(
                process,
                datetime.datetime.now(datetime.UTC),
                script_path,
            ),
        )
        return JobStartResult(
            job_id=process.pid,
            metadata={"pid": process.pid},
        )

    @override
    async def list_jobs(self, job_id: None | str = None) -> Iterable[Job]:
        result: list[Job] = []
        for wrapped_process in self._processes:
            rc = wrapped_process.process.returncode
            if rc is not None:
                await wrapped_process.script_path.unlink(missing_ok=True)
            result.append(
                Job(
                    id=wrapped_process.process.pid,
                    status=(
                        JobStatus.SUCCESSFUL
                        if rc is not None and rc == 0
                        else JobStatus.RUNNING
                        if rc is None
                        else JobStatus.FAILED
                    ),
                    started=wrapped_process.started,
                    metadata={"pid": wrapped_process.process.pid},
                ),
            )
        return result
