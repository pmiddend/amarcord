import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from amarcord.amici.workload_manager.job import Job
from amarcord.amici.workload_manager.job import JobMetadata
from amarcord.amici.workload_manager.workload_manager import JobStartError
from amarcord.amici.workload_manager.workload_manager import JobStartResult
from amarcord.amici.workload_manager.workload_manager import WorkloadManager


@dataclass(frozen=True)
class JobStart:
    working_directory: Path
    executable: Path
    command_line: str
    time_limit: datetime.timedelta
    extra_files: list[Path]


class DummyWorkloadManager(WorkloadManager):
    def __init__(self) -> None:
        super().__init__()
        self.job_starts: list[JobStart] = []
        self.job_start_results: list[None | JobStartResult] = []
        self.jobs: list[Job] = []

    async def start_job(
        self,
        working_directory: Path,
        executable: Path,
        command_line: str,
        time_limit: datetime.timedelta,
        stdout: None | Path = None,
        stderr: None | Path = None,
    ) -> JobStartResult:
        self.job_starts.append(
            JobStart(working_directory, executable, command_line, time_limit, [])
        )
        assert (
            self.job_start_results
        ), "No job start results left, so there was one more job start than anticipated"
        result = self.job_start_results.pop()
        if result is not None:
            return result
        raise JobStartError("some error")

    async def list_jobs(self) -> Iterable[Job]:
        return self.jobs

    def equals(self, metadata_a: JobMetadata, metadata_b: JobMetadata) -> bool:
        pass

    def is_our_job(self, metadata_a: JobMetadata) -> bool:
        pass

    def should_restart(self, job_id: int, output_directory: Path) -> bool:
        pass
