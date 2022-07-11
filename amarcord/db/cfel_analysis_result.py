import datetime
from dataclasses import dataclass

from amarcord.db.table_classes import DBFile


@dataclass(frozen=True, eq=True)
class DBCFELAnalysisResult:
    id: int | None
    directory_name: str
    data_set_id: int
    resolution: str
    rsplit: float
    cchalf: float
    ccstar: float
    snr: float
    completeness: float
    multiplicity: float
    total_measurements: int
    unique_reflections: int
    num_patterns: int
    num_hits: int
    indexed_patterns: int
    indexed_crystals: int
    crystfel_version: str
    ccstar_rsplit: float
    created: datetime.datetime
    files: list[DBFile]
