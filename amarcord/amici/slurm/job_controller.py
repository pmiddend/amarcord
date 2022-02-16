import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from typing import List
from typing import Protocol

from amarcord.amici.slurm.job import Job, JobMetadata


@dataclass(frozen=True)
class JobStartResult:
    metadata: JobMetadata
    output_directory: Path


class JobStartError(Exception):
    def __init__(self, message: str) -> None:
        super().__init__()
        self.message = message


class JobController(Protocol):
    async def start_job(
        self,
        path: Path,
        executable: Path,
        command_line: str,
        time_limit: datetime.timedelta,
        extra_files: List[Path],
    ) -> JobStartResult:
        ...

    async def list_jobs(self) -> Iterable[Job]:
        ...

    def equals(self, metadata_a: JobMetadata, metadata_b: JobMetadata) -> bool:
        ...

    def is_our_job(self, metadata_a: JobMetadata) -> bool:
        ...

    def should_restart(self, job_id: int, output_directory: Path) -> bool:
        ...
