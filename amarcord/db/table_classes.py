import datetime
from dataclasses import dataclass
from dataclasses import field
from typing import List
from typing import Optional

from amarcord.db.comment import DBComment
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.proposal_id import ProposalId
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
    proposal_id: ProposalId
    name: str
    short_name: str
    molecular_weight: Optional[float]
    uniprot_id: str


@dataclass(frozen=True)
class DBSample:
    id: Optional[int]
    proposal_id: ProposalId
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
    tag: Optional[str] = None
    comment: Optional[str] = None
    software_version: Optional[str] = None
    max_num_peaks: Optional[float] = None
    adc_threshold: Optional[float] = None
    minimum_snr: Optional[float] = None
    min_pixel_count: Optional[int] = None
    max_pixel_count: Optional[int] = None
    min_res: Optional[float] = None
    max_res: Optional[float] = None
    bad_pixel_map_filename: Optional[str] = None
    bad_pixel_map_hdf5_path: Optional[str] = None
    local_bg_radius: Optional[float] = None
    min_peak_over_neighbor: Optional[float] = None
    min_snr_biggest_pix: Optional[float] = None
    min_snr_peak_pix: Optional[float] = None
    min_sig: Optional[float] = None
    min_squared_gradient: Optional[float] = None
    geometry: Optional[str] = None


@dataclass(frozen=True, eq=True)
class DBHitFindingParameters:
    id: Optional[int] = field(compare=False)
    min_peaks: int
    tag: Optional[str]
    comment: Optional[str]
    software: str
    software_version: Optional[str] = None


@dataclass(frozen=True, eq=True)
class DBIndexingParameters:
    id: Optional[int] = field(compare=False)
    tag: Optional[str]
    comment: Optional[str]
    software: str
    software_version: Optional[str]
    command_line: str
    parameters: JSONDict
    methods: List[str]
    geometry: Optional[str]


@dataclass(frozen=True, eq=True)
class DBIntegrationParameters:
    id: Optional[int] = field(compare=False)
    tag: Optional[str]
    comment: Optional[str]
    software: str
    software_version: Optional[str]
    method: Optional[str]
    center_boxes: Optional[bool]
    overpredict: Optional[bool]
    push_res: Optional[float]
    radius_inner: Optional[int]
    radius_middle: Optional[int]
    radius_outer: Optional[int]


@dataclass(frozen=True, eq=True)
class DBIndexingResult:
    id: Optional[int] = field(compare=False)
    hit_finding_result_id: int = field(compare=False)
    peak_search_parameters_id: int = field(compare=False)
    indexing_parameters_id: int = field(compare=False)
    integration_parameters_id: int = field(compare=False)
    num_indexed: int
    num_crystals: int
    tag: Optional[str]
    comment: Optional[str]
    result_filename: Optional[str]


@dataclass(frozen=True, eq=True)
class DBHitFindingResult:
    id: Optional[int] = field(compare=False)
    peak_search_parameters_id: int = field(compare=False)
    hit_finding_parameters_id: int = field(compare=False)
    data_source_id: Optional[int] = field(compare=False)
    result_filename: str
    peaks_filename: Optional[str]
    result_type: str
    average_peaks_event: float
    average_resolution: float
    number_of_hits: int
    hit_rate: float
    tag: Optional[str] = None
    comment: Optional[str] = None


@dataclass(frozen=True, eq=True)
class DBDataSource:
    id: Optional[int] = field(compare=False)
    run_id: int
    number_of_frames: int
    source: Optional[JSONDict] = None
    tag: Optional[str] = None
    comment: Optional[str] = None


@dataclass(frozen=True, eq=True)
class DBLinkedIndexingResult:
    indexing_result: DBIndexingResult
    peak_search_parameters: DBPeakSearchParameters
    indexing_parameters: DBIndexingParameters
    integration_parameters: DBIntegrationParameters


@dataclass(frozen=True, eq=True)
class DBLinkedHitFindingResult:
    hit_finding_result: DBHitFindingResult
    peak_search_parameters: DBPeakSearchParameters
    hit_finding_parameters: DBHitFindingParameters
    indexing_results: List[DBLinkedIndexingResult]


@dataclass(frozen=True, eq=True)
class DBLinkedDataSource:
    data_source: DBDataSource
    hit_finding_results: List[DBLinkedHitFindingResult]


@dataclass(frozen=True)
class DBSampleAnalysisResult:
    sample_id: int
    sample_name: str
    data_sources: List[DBLinkedDataSource]
