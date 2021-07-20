import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import List
from typing import Optional

from amarcord.modules.json import JSONArray


@dataclass(frozen=True)
class DBTool:
    id: Optional[int]
    name: str
    executable_path: Path
    extra_files: List[Path]
    command_line: str
    description: str
    inputs: Optional[JSONArray] = None
    created: Optional[datetime.datetime] = None
