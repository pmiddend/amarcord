import datetime
from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from typing import Iterable

from anyio import Path

from amarcord.amici.workload_manager.job import Job
from amarcord.amici.workload_manager.job import JobMetadata


@dataclass(frozen=True)
class JobStartResult:
    job_id: int
    metadata: JobMetadata


class JobStartError(Exception):
    def __init__(self, message: str) -> None:
        super().__init__()
        self.message = message


class WorkloadManager(ABC):
    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    async def start_job(
        self,
        working_directory: Path,
        script: str,
        name: str,
        time_limit: datetime.timedelta,
        environment: dict[str, str],
        stdout: Path | None = None,
        stderr: Path | None = None,
    ) -> JobStartResult: ...

    @abstractmethod
    async def list_jobs(self, job_id: str | None = None) -> Iterable[Job]: ...
