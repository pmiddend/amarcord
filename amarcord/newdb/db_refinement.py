import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from amarcord.newdb.refinement_method import RefinementMethod


@dataclass(frozen=True)
class DBRefinement:
    refinement_id: Optional[int]
    data_reduction_id: Optional[int]
    analysis_time: datetime.datetime
    folder_path: Optional[Path]
    method: RefinementMethod
    initial_pdb_path: Optional[Path] = None
    final_pdb_path: Optional[Path] = None
    refinement_mtz_path: Optional[Path] = None
    comment: Optional[str] = None
    resolution_cut: Optional[float] = None
    rfree: Optional[float] = None
    rwork: Optional[float] = None
    rms_bond_length: Optional[float] = None
    rms_bond_angle: Optional[float] = None
    num_blobs: Optional[int] = None
    average_model_b: Optional[float] = None
