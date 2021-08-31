from dataclasses import dataclass
from typing import Dict
from typing import Optional

from amarcord.newdb.reduction_method import ReductionMethod


@dataclass(frozen=True)
class ReductionSimpleFilter:
    reduction_method: Optional[ReductionMethod]
    only_non_refined: bool
    crystal_filters: Dict[str, Optional[str]]
