import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from typing import Union

from amarcord.newdb.db import ReductionMethod


@dataclass(frozen=True)
class AnalysisResult:
    analysis_time: datetime.datetime
    base_path: Path
    mtz_file: Optional[Path]
    method: ReductionMethod
    resolution_cc: Optional[float]
    resolution_isigma: float
    a: float
    b: float
    c: float
    alpha: float
    beta: float
    gamma: float
    space_group: Union[int, str]
    isigi: Optional[float]
    rmeas: Optional[float]
    cchalf: Optional[float]
    rfactor: Optional[float]
    wilson_b: Optional[float]
