from dataclasses import dataclass
from typing import List

from amarcord.newdb.db_analysis_row import DBAnalysisRow


@dataclass(frozen=True)
class DBAnalysisResult:
    rows: List[DBAnalysisRow]
    columns: List[str]
    total_diffractions: int
    total_rows: int
