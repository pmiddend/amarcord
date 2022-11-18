from dataclasses import dataclass

from amarcord.amici.crystfel.util import CrystFELCellFile
from amarcord.db.merge_model import MergeModel
from amarcord.db.merge_negative_handling import MergeNegativeHandling
from amarcord.db.polarisation import Polarisation
from amarcord.db.scale_intensities import ScaleIntensities


@dataclass(frozen=True)
class DBMergeParameters:
    point_group: str
    cell_description: CrystFELCellFile
    negative_handling: None | MergeNegativeHandling
    merge_model: MergeModel
    scale_intensities: ScaleIntensities
    post_refinement: bool
    iterations: int
    polarisation: None | Polarisation
    start_after: None | int
    stop_after: None | int
    rel_b: float
    no_pr: bool
    force_bandwidth: None | float
    force_radius: None | float
    force_lambda: None | float
    no_delta_cc_half: bool
    max_adu: None | float
    min_measurements: int
    logs: bool
    min_res: None | float
    push_res: None | float
    w: None | str
