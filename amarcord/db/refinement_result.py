from dataclasses import dataclass


@dataclass(frozen=True, eq=True)
class DBRefinementResultOutput:
    id: int
    merge_result_id: int
    pdb_file_id: int
    mtz_file_id: int
    r_free: float
    r_work: float
    rms_bond_angle: float
    rms_bond_length: float
