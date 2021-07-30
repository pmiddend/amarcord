import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class RefinementResult:
    analysis_time: datetime.datetime
    folder_path: Path
    initial_pdb_path: Path
    final_pdb_path: Optional[Path]
    refinement_mtz_path: Optional[Path]
    comment: Optional[str]
    resolution_cut: Optional[float]
    rfree: Optional[float]
    rwork: Optional[float]
    rms_bond_length: Optional[float]
    rms_bond_angle: Optional[float]
    num_blobs: Optional[int]
    average_model_b: Optional[float]
