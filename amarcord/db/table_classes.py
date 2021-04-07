import datetime
from dataclasses import dataclass
from dataclasses import field
from typing import List
from typing import Optional

from amarcord.db.comment import DBComment
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.raw_attributi_map import RawAttributiMap
from amarcord.modules.json import JSONDict


@dataclass(frozen=True)
class DBEvent:
    id: int
    created: datetime.datetime
    level: EventLogLevel
    source: str
    text: str


@dataclass(frozen=True)
class DBTarget:
    id: Optional[int]
    name: str
    short_name: str
    molecular_weight: Optional[float]
    uniprot_id: str


@dataclass(frozen=True)
class DBSample:
    id: Optional[int]
    name: str
    attributi: RawAttributiMap
    target_id: Optional[int] = None
    compounds: Optional[List[int]] = None
    micrograph: Optional[str] = None
    protocol: Optional[str] = None


@dataclass(frozen=True)
class DBRun:
    attributi: RawAttributiMap
    id: int
    sample_id: Optional[int]
    proposal_id: int
    modified: datetime.datetime
    comments: List[DBComment]


@dataclass(frozen=True, eq=True)
class DBPeakSearchParameters:
    id: Optional[int] = field(compare=False)
    method: str
    software: str
    command_line: str
    tag: Optional[str] = None
    comment: Optional[str] = None
    software_version: Optional[str] = None
    software_git_repository: Optional[str] = None
    software_git_sha: Optional[str] = None
    max_num_peaks: Optional[float] = None
    adc_threshold: Optional[float] = None
    minimum_snr: Optional[float] = None
    min_pixel_count: Optional[float] = None
    max_pixel_count: Optional[float] = None
    min_res: Optional[float] = None
    max_res: Optional[float] = None
    bad_pixel_filename: Optional[str] = None
    local_bg_radius: Optional[float] = None
    min_peak_over_neighbor: Optional[float] = None
    min_snr_biggest_pix: Optional[float] = None
    min_snr_peak_pix: Optional[float] = None
    min_sig: Optional[float] = None
    min_squared_gradient: Optional[float] = None
    geometry_filename: Optional[str] = None


@dataclass(frozen=True, eq=True)
class DBHitFindingParameters:
    id: Optional[int] = field(compare=False)
    min_peaks: int
    tag: Optional[str]
    comment: Optional[str]


@dataclass(frozen=True, eq=True)
class DBIndexingParameters:
    software: str
    command_line: str
    parameters: JSONDict


@dataclass(frozen=True, eq=True)
class DBIntegrationParameters:
    pass


@dataclass(frozen=True, eq=True)
class DBAmbiguityParameters:
    pass


@dataclass(frozen=True, eq=True)
class DBMergeParameters:
    software: str
    command_line: str
    parameters: JSONDict


@dataclass(frozen=True, eq=True)
class DBMergeResult:
    id: Optional[int]
    merge_parameters: DBMergeParameters
    indexing_result_ids: List[int] = field(compare=False)
    rsplit: float
    cc_half: float


@dataclass(frozen=True, eq=True)
class DBIndexingResult:
    id: Optional[int]
    hit_finding_results_id: int = field(compare=False)
    indexing_parameters: DBIndexingParameters
    integration_parameters: DBIntegrationParameters
    ambiguity_parameters: Optional[DBAmbiguityParameters]
    num_indexed: int
    num_crystals: int
    tag: Optional[str]
    comment: Optional[str]


@dataclass(frozen=True, eq=True)
class DBHitFindingResult:
    id: Optional[int] = field(compare=False)
    data_source_id: Optional[int] = field(compare=False)
    peak_search_parameters: DBPeakSearchParameters
    hit_finding_parameters: DBHitFindingParameters
    result_filename: str
    number_of_hits: int
    hit_rate: float
    indexing_results: List[DBIndexingResult] = field(compare=False)
    tag: Optional[str] = None
    comment: Optional[str] = None


@dataclass(frozen=True)
class DBDataSource:
    id: Optional[int] = field(compare=False)
    run_id: int
    number_of_frames: int
    hit_finding_results: List[DBHitFindingResult] = field(compare=False)
    source: Optional[JSONDict] = None
    tag: Optional[str] = None
    comment: Optional[str] = None


@dataclass(frozen=True)
class DBSampleAnalysisResult:
    sample_id: int
    sample_name: str
    indexing_paths: List[DBDataSource]
    merge_results: List[DBMergeResult]
