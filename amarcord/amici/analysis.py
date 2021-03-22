import os
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import List

from amarcord.db.db import DB
from amarcord.db.tables import create_tables
from amarcord.modules.dbcontext import CreationMode
from amarcord.modules.dbcontext import DBContext


@dataclass(frozen=True)
class DataSource:
    run_id: int
    number_of_frames: int
    train_ids: List[str] = field(default_factory=lambda: [])
    source: List[str] = field(default_factory=lambda: [])
    tag: str = ""
    comment: str = ""


@dataclass(frozen=True)
class PeakSearchParameters:
    max_num_peaks: float
    adc_threshold: float
    minimum_snr: float
    min_pixel_count: int
    max_pixel_count: int
    local_bg_radius: float
    min_res: float
    max_res: float
    bad_pixel_map_filename: str
    bad_pixel_map_hdf5_path: str
    min_peak_over_neighbour: float
    min_snr_biggest_pix: float
    min_snr_peak_pix: float
    min_sig: float
    min_squared_gradient: float
    geometry: str
    geometry_filename: str = ""
    tag: str = ""
    comment: str = ""


@dataclass(frozen=True)
class HitFindingParameters:
    min_peaks: int


@dataclass(frozen=True)
class HitFindingResults:
    number_hits: int
    hit_rate: float
    result_filename: str
    tag: str = ""
    comment: str = ""


def ingest_cheetah(
    data_source: DataSource,
    peak_search_parameters: PeakSearchParameters,
    hit_finding_parameters: HitFindingParameters,
    hit_finding_results: HitFindingResults,
) -> None:
    dbcontext = DBContext(
        "sqlite:////" + str(Path(os.environ["HOME"]) / "amarcord-test.sqlite")
    )
    tables = create_tables(dbcontext)
    DB(dbcontext, tables)

    dbcontext.create_all(creation_mode=CreationMode.CHECK_FIRST)

    print(
        f"ingesting from {data_source}, {peak_search_parameters}, {hit_finding_parameters}, {hit_finding_results}..."
    )
