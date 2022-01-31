from dataclasses import dataclass


@dataclass(frozen=True)
class DBCFELAnalysisResult:
    directory_name: str
    run_from: int
    run_to: int
    resolution: str
    rsplit: float
    cchalf: float
    ccstar: float
    snr: float
    completeness: float
    multiplicity: float
    total_measurements: int
    unique_reflections: int
    wilson_b: float
    outer_shell: str
    num_patterns: int
    num_hits: int
    indexed_patterns: int
    indexed_crystals: int
    comment: str
