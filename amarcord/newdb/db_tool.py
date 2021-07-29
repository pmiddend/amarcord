import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import List
from typing import Optional

from amarcord.workflows.command_line import CommandLine


@dataclass(frozen=True)
class DBTool:
    id: Optional[int]
    name: str
    executable_path: Path
    extra_files: List[Path]
    command_line: str
    description: str
    inputs: Optional[CommandLine] = None
    created: Optional[datetime.datetime] = None
