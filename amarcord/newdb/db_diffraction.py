import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from amarcord.newdb.beamline import Beamline
from amarcord.newdb.diffraction_type import DiffractionType


@dataclass(frozen=True)
class DBDiffraction:
    crystal_id: str
    run_id: int
    diffraction: Optional[DiffractionType]
    created: Optional[datetime.datetime] = None
    dewar_position: Optional[int] = None
    beamline: Optional[Beamline] = None
    beam_intensity: Optional[str] = None
    pinhole: Optional[str] = None
    focusing: Optional[str] = None
    comment: Optional[str] = None
    estimated_resolution: Optional[str] = None
    metadata: Optional[str] = None
    angle_start: Optional[float] = None
    number_of_frames: Optional[int] = None
    angle_step: Optional[float] = None
    exposure_time: Optional[float] = None
    xray_energy: Optional[float] = None
    xray_wavelength: Optional[float] = None
    detector_name: Optional[str] = None
    detector_distance: Optional[float] = None
    detector_edge_resolution: Optional[float] = None
    aperture_radius: Optional[float] = None
    filter_transmission: Optional[float] = None
    ring_current: Optional[float] = None
    data_raw_filename_pattern: Optional[Path] = None
    microscope_image_filename_pattern: Optional[Path] = None
    aperture_horizontal: Optional[float] = None
    aperture_vertical: Optional[float] = None
