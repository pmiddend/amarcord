from dataclasses import dataclass
from typing import Optional

from amarcord.newdb.diffraction_type import DiffractionType


@dataclass(frozen=True)
class DBBeamlineDiffraction:
    crystal_id: str
    run_id: int
    diffraction: Optional[DiffractionType]
    comment: str
    beam_intensity: Optional[str]
    puck_position_id: int
    dewar_position: int
