from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from typing import List
from typing import Protocol

from amarcord.modules.json import JSONDict
from amarcord.workflows.job import Job


@dataclass(frozen=True)
class JobStartResult:
    metadata: JSONDict
    output_directory: Path


class JobStartError(Exception):
    def __init__(self, message: str) -> None:
        super().__init__()
        self.message = message


class JobController(Protocol):
    def start_job(
        self,
        relative_sub_path: Path,
        #        job_description: str,
        executable: Path,
        command_line: str,
        extra_files: List[Path],
    ) -> JobStartResult:
        ...

    def list_jobs(self) -> Iterable[Job]:
        ...

    def equals(self, metadata_a: JSONDict, metadata_b: JSONDict) -> bool:
        ...
