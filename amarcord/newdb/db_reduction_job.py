from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from typing import Union

from amarcord.newdb.db_job import DBJob
from amarcord.newdb.db_tool import DBTool


@dataclass(frozen=True)
class DBMiniDiffraction:
    run_id: int
    crystal_id: str
    data_raw_filename_pattern: Optional[Path]
    resulting_data_reduction_id: Optional[int]


@dataclass(frozen=True)
class DBMiniReduction:
    crystal_id: str
    run_id: int
    data_reduction_id: int
    mtz_path: Path
    resulting_refinement_id: Optional[int]


@dataclass(frozen=True)
class DBJobWithInputsAndOutputs:
    job: DBJob
    tool: DBTool
    io: Union[DBMiniDiffraction, DBMiniReduction]
