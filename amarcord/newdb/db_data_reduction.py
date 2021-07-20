import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from typing import Union

from amarcord.newdb.reduction_method import ReductionMethod


@dataclass(frozen=True)
class DBDataReduction:
    data_reduction_id: Optional[int]
    crystal_id: str
    run_id: int
    analysis_time: datetime.datetime
    method: ReductionMethod
    folder_path: Path
    mtz_path: Optional[Path] = None
    comment: Optional[str] = None
    resolution_cc: Optional[float] = None
    resolution_isigma: Optional[float] = None
    a: Optional[float] = None
    b: Optional[float] = None
    c: Optional[float] = None
    alpha: Optional[float] = None
    beta: Optional[float] = None
    gamma: Optional[float] = None
    space_group: Union[int, str, None] = None
    isigi: Optional[float] = None
    rmeas: Optional[float] = None
    cchalf: Optional[float] = None
    rfactor: Optional[float] = None
    wilson_b: Optional[float] = None
