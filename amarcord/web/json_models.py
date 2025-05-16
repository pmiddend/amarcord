from enum import Enum
from typing import Annotated

from pydantic import BaseModel
from pydantic import Field

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributo_value import AttributoValue
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.chemical_type import ChemicalType
from amarcord.db.db_job_status import DBJobStatus
from amarcord.db.merge_model import MergeModel
from amarcord.db.merge_negative_handling import MergeNegativeHandling
from amarcord.db.merge_result import JsonMergeResultInternal
from amarcord.db.orm import AlignDetectorGroup
from amarcord.db.run_internal_id import RunInternalId
from amarcord.db.scale_intensities import ScaleIntensities
from amarcord.json_schema import JSONSchemaArray
from amarcord.json_schema import JSONSchemaBoolean
from amarcord.json_schema import JSONSchemaInteger
from amarcord.json_schema import JSONSchemaNumber
from amarcord.json_schema import JSONSchemaString
from amarcord.json_schema import JSONSchemaUnion


class JsonUpdateBeamtimeInput(BaseModel):
    # For creating beamtimes, we set id=0. For updates, the actual ID.
    id: BeamtimeId
    external_id: str
    beamline: str
    proposal: str
    title: str
    comment: str
    start_local: int
    end_local: int
    analysis_output_path: str


# In case you're wondering why geometries are a little different w.r.t
# creation and updates: we tried a new scheme here that makes more sense.
class JsonGeometryUpdate(BaseModel):
    content: str
    name: str


class JsonGeometryCopyToBeamtime(BaseModel):
    geometry_id: int
    target_beamtime_id: BeamtimeId


class JsonGeometryCreate(BaseModel):
    beamtime_id: BeamtimeId
    content: str
    name: str


class JsonGeometryWithUsages(BaseModel):
    geometry_id: int
    usages: int


class JsonReadSingleGeometryOutput(BaseModel):
    content: str


class JsonReadGeometriesForSingleBeamtime(BaseModel):
    geometries: list["JsonGeometryWithoutContent"]
    geometry_with_usage: list[JsonGeometryWithUsages]


class JsonReadGeometriesForAllBeamtimes(BaseModel):
    geometries: list["JsonGeometryWithoutContent"]
    beamtimes: list["JsonBeamtimeOutput"]


class JsonGeometryWithContent(BaseModel):
    id: int
    beamtime_id: BeamtimeId
    content: str
    hash: str
    name: str
    created: int
    created_local: int


class JsonGeometryWithoutContent(BaseModel):
    id: int
    beamtime_id: BeamtimeId
    hash: str
    name: str
    created: int
    created_local: int


class JsonBeamtimeInput(BaseModel):
    id: BeamtimeId
    external_id: str
    proposal: str
    beamline: str
    title: str
    comment: str
    start_local: int
    end_local: int
    analysis_output_path: str


class JsonBeamtimeOutput(BaseModel):
    id: BeamtimeId
    external_id: str
    proposal: str
    beamline: str
    title: str
    comment: str
    start: int
    start_local: int
    end: int
    end_local: int
    chemical_names: list[str]
    analysis_output_path: str


class JsonReadBeamtime(BaseModel):
    beamtimes: list[JsonBeamtimeOutput]


class JsonEventInput(BaseModel):
    source: str
    text: str
    level: str
    file_ids: list[int]


class JsonFileOutput(BaseModel):
    id: int
    description: str
    type_: str
    original_path: None | str = None
    file_name: str
    size_in_bytes: int
    size_in_bytes_compressed: None | int = None


class JsonEvent(BaseModel):
    id: int
    source: str
    text: str
    created: int
    created_local: int
    level: str
    files: list[JsonFileOutput]


class JsonEventTopLevelInput(BaseModel):
    beamtime_id: BeamtimeId
    event: JsonEventInput
    with_live_stream: bool


class JsonEventTopLevelOutput(BaseModel):
    id: int


class JsonAttributoValue(BaseModel):
    attributo_id: int
    # These types mirror the types of "AttributoValue"
    attributo_value_str: None | str = None
    attributo_value_int: None | int = None
    attributo_value_chemical: None | int = None
    attributo_value_datetime: None | int = None
    attributo_value_datetime_local: None | int = None
    attributo_value_float: None | float = None
    attributo_value_bool: None | bool = None
    attributo_value_list_str: None | list[str] = None
    attributo_value_list_float: None | list[float] = None
    attributo_value_list_bool: None | list[bool] = None

    def to_attributo_value(self) -> AttributoValue:
        return (
            self.attributo_value_str
            if self.attributo_value_str is not None
            else (
                self.attributo_value_int
                if self.attributo_value_int is not None
                else (
                    self.attributo_value_float
                    if self.attributo_value_float is not None
                    else (
                        self.attributo_value_bool
                        if self.attributo_value_bool is not None
                        else (
                            self.attributo_value_list_str
                            if self.attributo_value_list_str is not None
                            else (
                                self.attributo_value_list_float
                                if self.attributo_value_list_float is not None
                                else (
                                    self.attributo_value_chemical
                                    if self.attributo_value_chemical is not None
                                    else (
                                        self.attributo_value_datetime
                                        if self.attributo_value_datetime is not None
                                        else None
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )


class JsonChemicalWithoutId(BaseModel):
    beamtime_id: BeamtimeId
    name: str
    responsible_person: str
    chemical_type: ChemicalType
    attributi: list[JsonAttributoValue]
    file_ids: list[int]


class JsonChemicalWithId(BaseModel):
    id: int
    beamtime_id: BeamtimeId
    name: str
    responsible_person: str
    chemical_type: ChemicalType
    attributi: list[JsonAttributoValue]
    file_ids: list[int]


class JsonCreateChemicalOutput(BaseModel):
    id: int


class JsonDeleteChemicalInput(BaseModel):
    id: int


class JsonDeleteChemicalOutput(BaseModel):
    id: int


class JsonChemical(BaseModel):
    id: int
    beamtime_id: BeamtimeId
    name: str
    responsible_person: str
    chemical_type: ChemicalType
    attributi: list[JsonAttributoValue]
    files: list[JsonFileOutput]


class JsonRefinementResult(BaseModel):
    id: int
    merge_result_id: int
    pdb_file_id: int
    mtz_file_id: int
    r_free: float
    r_work: float
    rms_bond_angle: float
    rms_bond_length: float


class JsonChemicalIdAndName(BaseModel):
    chemical_id: int
    name: str


class JsonMergeResultStateQueued(BaseModel):
    queued: bool


class JsonMergeResultStateError(BaseModel):
    started: int
    started_local: int
    stopped: int
    stopped_local: int
    error: str
    latest_log: str


class JsonMergeResultStateRunning(BaseModel):
    started: int
    started_local: int
    job_id: int
    latest_log: str


class JsonMergeResultStateDone(BaseModel):
    started: int
    started_local: int
    stopped: int
    stopped_local: int
    result: JsonMergeResultInternal


class JsonPolarisation(BaseModel):
    angle: int
    percent: int


class JsonMergeParameters(BaseModel):
    # Beware: this can be empty, in the case of the creation of a new merge job.
    # Then we take the point group from the chemical information.
    #
    # We could have made this "None | str" but that complicates things. For now, do it this way.
    point_group: str
    space_group: None | str = None
    # Same as point_group comment above.
    cell_description: str
    negative_handling: None | MergeNegativeHandling = None
    merge_model: MergeModel
    scale_intensities: ScaleIntensities
    post_refinement: bool
    iterations: int
    polarisation: None | JsonPolarisation = None
    start_after: None | int = None
    stop_after: None | int = None
    rel_b: float
    no_pr: bool
    force_bandwidth: None | float = None
    force_radius: None | float = None
    force_lambda: None | float = None
    no_delta_cc_half: bool
    max_adu: None | float = None
    min_measurements: int
    logs: bool
    min_res: None | float = None
    push_res: None | float = None
    w: None | str = None
    ambigator_command_line: str
    cutoff_lowres: None | float = None
    cutoff_highres: Annotated[None | list[float], Field(min_length=1, max_length=3)] = (
        None
    )


class JsonMergeResult(BaseModel):
    id: int
    created: int
    created_local: int
    runs: list[str]
    indexing_result_ids: list[int]
    state_queued: None | JsonMergeResultStateQueued = None
    state_error: None | JsonMergeResultStateError = None
    state_running: None | JsonMergeResultStateRunning = None
    state_done: None | JsonMergeResultStateDone = None
    parameters: JsonMergeParameters
    refinement_results: list[JsonRefinementResult]


class JsonAttributo(BaseModel):
    id: int
    name: str
    description: str
    group: str
    associated_table: AssociatedTable
    attributo_type_integer: None | JSONSchemaInteger = None
    attributo_type_number: None | JSONSchemaNumber = None
    attributo_type_string: None | JSONSchemaString = None
    attributo_type_array: None | JSONSchemaArray = None
    attributo_type_boolean: None | JSONSchemaBoolean = None


class JsonReadChemicals(BaseModel):
    chemicals: list[JsonChemical]
    attributi: list[JsonAttributo]


class JsonAttributoWithName(BaseModel):
    id: int
    name: str


class JsonReadAllChemicals(BaseModel):
    chemicals: list[JsonChemical]
    beamtimes: list[JsonBeamtimeOutput]
    attributi_names: list[JsonAttributoWithName]


class JsonCopyChemicalInput(BaseModel):
    chemical_id: int
    target_beamtime_id: int
    create_attributi: bool


class JsonCopyChemicalOutput(BaseModel):
    new_chemical_id: int


class JsonIndexingParameters(BaseModel):
    id: None | int = None
    cell_description: None | str = None
    is_online: bool
    command_line: str
    geometry_id: None | int


class JsonUpdateOnlineIndexingParametersInput(BaseModel):
    command_line: str
    geometry_id: int
    source: str


class JsonUpdateOnlineIndexingParametersOutput(BaseModel):
    success: bool


class JsonAlignDetectorGroup(BaseModel):
    group: str
    x_translation_mm: float
    y_translation_mm: float
    z_translation_mm: None | float = None
    x_rotation_deg: None | float = None
    y_rotation_deg: None | float = None


class JsonIndexingResult(BaseModel):
    id: int
    created: int
    created_local: int
    started: None | int = None
    started_local: None | int = None
    stopped: None | int = None
    stopped_local: None | int = None
    parameters: JsonIndexingParameters
    stream_file: str
    program_version: str
    run_internal_id: int
    run_external_id: int
    frames: int
    hits: int
    indexed_frames: int
    indexed_crystals: int
    status: DBJobStatus
    align_detector_groups: list[JsonAlignDetectorGroup]
    generated_geometry_id: None | int
    unit_cell_histograms_file_id: None | int = None
    has_error: bool
    # Commented out, we retrieve the log separately
    # latest_log: str


class JsonCreateIndexingForDataSetInput(BaseModel):
    data_set_id: int
    is_online: bool
    cell_description: str
    geometry_id: int
    command_line: str
    source: str


class JsonImportFinishedIndexingJobInput(BaseModel):
    is_online: bool
    cell_description: str
    command_line: str
    source: str
    run_internal_id: int
    stream_file: str
    program_version: str
    frames: int
    hits: int
    indexed_frames: int
    align_detector_groups: list[AlignDetectorGroup]
    geometry_contents: str
    generated_geometry_file: None | str = None
    job_log: str


class JsonImportFinishedIndexingJobOutput(BaseModel):
    indexing_result_id: int


class JsonReadIndexingParametersOutput(BaseModel):
    data_set_id: int
    cell_description: str
    sources: list[str]


class JsonCreateIndexingForDataSetOutput(BaseModel):
    jobs_started_run_external_ids: list[int]
    indexing_result_id: int
    # used to display "job submitted" only on the specific data set
    data_set_id: int
    # we use that to scroll to the indexing job just created (and expand it)
    indexing_parameters_id: int


class JsonIndexingJobCreateInput(BaseModel):
    run_internal_id: int


class JsonIndexingJobCreateOutput(BaseModel):
    indexing_result_id: int


class JsonIndexingResultFinishSuccessfully(BaseModel):
    workload_manager_job_id: int
    stream_file: str
    program_version: str
    frames: int
    hits: int
    indexed_frames: int
    indexed_crystals: int
    align_detector_groups: list[JsonAlignDetectorGroup]
    generated_geometry_contents: str
    unit_cell_histograms_id: None | int = None

    # None, in this case, means "don't change/append to the log"
    latest_log: None | str = None


class JsonIndexingResultFinishWithError(BaseModel):
    error_message: str
    latest_log: str
    # The job might fail even before it gets to the workload manager (Slurm), in which case
    # we have no job ID.
    workload_manager_job_id: None | int = None


class JsonIndexingJobUpdateOutput(BaseModel):
    result: bool


class JsonMergeJobFinishOutput(BaseModel):
    result: bool


class JsonQueueMergeJobInput(BaseModel):
    strict_mode: bool
    indexing_parameters_id: int
    data_set_id: int
    merge_parameters: JsonMergeParameters


class JsonQueueMergeJobOutput(BaseModel):
    merge_result_id: int


class JsonStartRunOutput(BaseModel):
    run_internal_id: RunInternalId


class JsonStopRunOutput(BaseModel):
    result: bool


class JsonRunFile(BaseModel):
    id: int
    glob: str
    source: str


class JsonCreateOrUpdateRun(BaseModel):
    beamtime_id: BeamtimeId
    attributi: list[JsonAttributoValue]
    files: None | list[JsonRunFile] = None
    started: None | int = None
    stopped: None | int = None
    is_utc: bool = True
    create_data_set: bool = False


class JsonCreateOrUpdateRunOutput(BaseModel):
    run_created: bool
    indexing_result_id: None | int = None
    error_message: None | str = None
    run_internal_id: None | RunInternalId = None
    files: list[JsonRunFile]


class JsonUpdateRun(BaseModel):
    id: RunInternalId
    experiment_type_id: int
    attributi: list[JsonAttributoValue]
    files: None | list[JsonRunFile] = None


class JsonUpdateRunOutput(BaseModel):
    result: bool
    files: list[JsonRunFile]


class JsonReadRunsBulkInput(BaseModel):
    beamtime_id: BeamtimeId
    external_run_ids: list[int]


class JsonAttributoBulkValue(BaseModel):
    attributo_id: int
    values: list[JsonAttributoValue]


class JsonAttributiIdAndRole(BaseModel):
    id: int
    role: ChemicalType


class JsonExperimentType(BaseModel):
    id: int
    name: str
    attributi: list[JsonAttributiIdAndRole]


class JsonReadRunsBulkOutput(BaseModel):
    chemicals: list[JsonChemical]
    attributi: list[JsonAttributo]
    attributi_values: list[JsonAttributoBulkValue]
    experiment_types: list[JsonExperimentType]
    experiment_type_ids: list[int]


class JsonUpdateRunsBulkInput(BaseModel):
    beamtime_id: BeamtimeId
    external_run_ids: list[int]
    attributi: list[JsonAttributoValue]
    new_experiment_type_id: None | int = None


class JsonUpdateRunsBulkOutput(BaseModel):
    result: bool


class JsonAnalysisRun(BaseModel):
    id: int
    external_id: int
    data_set_id: None | int
    attributi: list[JsonAttributoValue]
    file_paths: list[JsonRunFile]


class JsonIndexingFom(BaseModel):
    hit_rate: float
    indexing_rate: float
    indexed_frames: int
    align_detector_groups: list[JsonAlignDetectorGroup]


class JsonIndexingResultStillRunning(BaseModel):
    workload_manager_job_id: int
    stream_file: str
    frames: int
    hits: int
    indexed_frames: int
    indexed_crystals: int
    # can be missing, in case we don't have that information but still want to signal progress
    job_started: None | int = None
    # None, in this case, means "don't change/append to the log"
    latest_log: None | str = None


class JsonIndexingStatistic(BaseModel):
    time: int
    frames: int
    hits: int
    indexed: int
    crystals: int


class JsonRunAnalysisIndexingResult(BaseModel):
    indexing_result_id: int
    run_id: int
    foms: JsonIndexingFom
    indexing_statistics: list[JsonIndexingStatistic]
    running: bool
    frames: None | int = None
    total_frames: None | int = None


class JsonDetectorShift(BaseModel):
    run_external_id: int
    run_start: int
    run_start_local: int
    run_end: None | int = None
    run_end_local: None | int = None
    align_detector_groups: list[JsonAlignDetectorGroup]
    geometry_id: None | int = None


class JsonReadBeamtimeGeometryDetails(BaseModel):
    detector_shifts: list[JsonDetectorShift]


class JsonRunId(BaseModel):
    internal_run_id: int
    external_run_id: int


class JsonReadRunAnalysis(BaseModel):
    chemicals: list[JsonChemical]
    attributi: list[JsonAttributo]
    run: None | JsonAnalysisRun = None
    run_ids: list[JsonRunId]
    indexing_results: list[JsonRunAnalysisIndexingResult]


class JsonDataSet(BaseModel):
    id: int
    experiment_type_id: int
    beamtime_id: int
    attributi: list[JsonAttributoValue]


class JsonDataSetStatistics(BaseModel):
    data_set_id: int
    run_count: int
    merge_results_count: int
    indexed_frames: int
    merge_or_indexing_jobs_running: bool


class JsonRunsBulkImportOutput(BaseModel):
    simulated: bool
    create_data_sets: bool
    errors: list[str]
    warnings: list[str]
    number_of_runs: int
    data_sets: list[JsonDataSet]


class JsonRunsBulkImportInfo(BaseModel):
    run_attributi: list[JsonAttributo]
    experiment_types: list[str]
    chemicals: list[JsonChemical]


class JsonRun(BaseModel):
    id: int
    external_id: int
    attributi: list[JsonAttributoValue]
    started: int
    started_local: int
    stopped: None | int = None
    stopped_local: None | int = None
    files: list[JsonRunFile]
    summary: JsonIndexingFom
    experiment_type_id: int


class JsonUserConfig(BaseModel):
    online_crystfel: bool
    auto_pilot: bool
    current_experiment_type_id: None | int = None
    current_online_indexing_parameters_id: None | int = None


class JsonLiveStream(BaseModel):
    file_id: int
    modified: int
    modified_local: int


class JsonDataSetWithFom(BaseModel):
    data_set: JsonDataSet
    fom: JsonIndexingFom


class JsonReadRuns(BaseModel):
    filter_dates: list[str]
    runs: list[JsonRun]
    attributi: list[JsonAttributo]
    experiment_types: list[JsonExperimentType]
    events: list[JsonEvent]
    chemicals: list[JsonChemical]


class JsonReadRunsOverview(BaseModel):
    live_stream: None | JsonLiveStream = None
    attributi: list[JsonAttributo]
    latest_indexing_result: None | JsonRunAnalysisIndexingResult = None
    latest_run: None | JsonRun = None
    foms_for_this_data_set: None | JsonDataSetWithFom = None
    # for the "choose ET" dropdown
    experiment_types: list[JsonExperimentType]
    events: list[JsonEvent]
    chemicals: list[JsonChemical]
    user_config: JsonUserConfig
    current_beamtime_user: None | str = None


class JsonCreateFileOutput(BaseModel):
    id: int
    file_name: str
    description: str
    type_: str
    size_in_bytes: int
    size_in_bytes_compressed: None | int
    original_path: None | str = None


class JsonUserConfigurationSingleOutput(BaseModel):
    value_bool: None | bool = None
    value_int: None | int = None


class JsonCreateExperimentTypeInput(BaseModel):
    name: str
    beamtime_id: BeamtimeId
    attributi: list[JsonAttributiIdAndRole]


class JsonCreateExperimentTypeOutput(BaseModel):
    id: int


class JsonChangeRunExperimentType(BaseModel):
    run_internal_id: int
    experiment_type_id: None | int = None


class JsonChangeRunExperimentTypeOutput(BaseModel):
    result: bool


class JsonExperimentTypeAndRuns(BaseModel):
    id: int
    runs: list[str]
    number_of_runs: int


class JsonReadExperimentTypes(BaseModel):
    experiment_types: list[JsonExperimentType]
    attributi: list[JsonAttributo]
    experiment_type_id_to_run: list[JsonExperimentTypeAndRuns]
    current_experiment_type_id: None | int = None


class JsonDeleteExperimentType(BaseModel):
    id: int


class JsonDeleteExperimentTypeOutput(BaseModel):
    result: bool


class JsonBeamtimeScheduleRowInput(BaseModel):
    users: str
    date: str
    shift: str
    comment: str
    td_support: str
    chemicals: list[int]


class JsonBeamtimeScheduleRowOutput(BaseModel):
    users: str
    date: str
    shift: str
    comment: str
    td_support: str
    chemicals: list[int]
    start: int
    start_local: int
    stop: int
    stop_local: int


class JsonUpdateBeamtimeScheduleInput(BaseModel):
    beamtime_id: BeamtimeId
    schedule: list[JsonBeamtimeScheduleRowInput]


class JsonBeamtimeScheduleOutput(BaseModel):
    schedule: list[JsonBeamtimeScheduleRowOutput]


class JsonUpdateLiveStream(BaseModel):
    id: int


class JsonCreateDataSetFromRun(BaseModel):
    run_internal_id: int


class JsonCreateDataSetFromRunOutput(BaseModel):
    data_set_id: int


class JsonCreateRefinementResultInput(BaseModel):
    merge_result_id: int
    pdb_file_id: int
    mtz_file_id: int
    r_free: float
    r_work: float
    rms_bond_angle: float
    rms_bond_length: float


class JsonCreateRefinementResultOutput(BaseModel):
    id: int


class JsonCreateDataSetInput(BaseModel):
    experiment_type_id: int
    attributi: list[JsonAttributoValue]


class JsonCreateDataSetOutput(BaseModel):
    id: int


class JsonReadDataSets(BaseModel):
    data_sets: list[JsonDataSet]
    chemicals: list[JsonChemical]
    attributi: list[JsonAttributo]
    experiment_types: list[JsonExperimentType]


class JsonDeleteDataSetInput(BaseModel):
    id: int


class JsonDeleteDataSetOutput(BaseModel):
    result: bool


class JsonDeleteEventInput(BaseModel):
    id: int


class JsonDeleteEventOutput(BaseModel):
    result: bool


class JsonReadEvents(BaseModel):
    events: list[JsonEvent]
    filter_dates: list[str]


class JsonDeleteFileInput(BaseModel):
    id: int


class JsonDeleteFileOutput(BaseModel):
    id: int


class JsonCreateAttributiFromSchemaSingleAttributo(BaseModel):
    attributo_name: str
    attributo_type: JSONSchemaUnion = Field(discriminator="type")
    description: None | str = None


class JsonCreateAttributiFromSchemaInput(BaseModel):
    attributi_schema: list[JsonCreateAttributiFromSchemaSingleAttributo]
    beamtime_id: BeamtimeId


class JsonCreateAttributiFromSchemaOutput(BaseModel):
    created_attributi: int


class JsonCreateAttributoInput(BaseModel):
    beamtime_id: BeamtimeId
    name: str
    description: str
    group: str
    associated_table: AssociatedTable
    attributo_type_integer: None | JSONSchemaInteger = None
    attributo_type_number: None | JSONSchemaNumber = None
    attributo_type_string: None | JSONSchemaString = None
    attributo_type_array: None | JSONSchemaArray = None
    attributo_type_boolean: None | JSONSchemaBoolean = None


class JsonCreateAttributoOutput(BaseModel):
    id: int


class JsonUpdateAttributoConversionFlags(BaseModel):
    ignore_units: bool


class JsonUpdateAttributoInput(BaseModel):
    attributo: JsonAttributo
    conversion_flags: JsonUpdateAttributoConversionFlags


class JsonUpdateAttributoOutput(BaseModel):
    id: int


class JsonDeleteAttributoInput(BaseModel):
    id: int


class JsonDeleteAttributoOutput(BaseModel):
    id: int


class JsonReadAttributi(BaseModel):
    attributi: list[JsonAttributo]


class JsonCheckStandardUnitInput(BaseModel):
    input: str


class JsonCheckStandardUnitOutput(BaseModel):
    input: str
    error: None | str = None
    normalized: None | str = None


class JsonIndexingParametersWithResults(BaseModel):
    parameters: JsonIndexingParameters
    indexing_results: list[JsonIndexingResult]
    merge_results: list[JsonMergeResult]


class JsonDataSetWithoutIndexingResults(BaseModel):
    data_set: JsonDataSet
    # To start indexing jobs
    internal_run_ids: list[int]
    # For display
    runs: list[str]


class JsonRunRange(BaseModel):
    run_from: int
    run_to: int


class JsonDataSetWithIndexingResults(BaseModel):
    data_set: JsonDataSet
    # To start indexing jobs
    internal_run_ids: list[int]
    # For display
    runs: list[JsonRunRange]
    point_group: str
    space_group: str
    cell_description: str
    indexing_results: list[JsonIndexingParametersWithResults]


class JsonReadSingleDataSetResults(BaseModel):
    attributi: list[JsonAttributo]
    chemical_id_to_name: list[JsonChemicalIdAndName]
    experiment_type: JsonExperimentType
    data_set: JsonDataSetWithIndexingResults


class JsonMergeStatus(str, Enum):
    BOTH = "both"
    UNMERGED = "unmerged"
    MERGED = "merged"


class JsonReadNewAnalysisInput(BaseModel):
    attributi_filter: list[JsonAttributoValue]
    beamtime_id: None | int = None
    merge_status: JsonMergeStatus


class JsonExperimentTypeWithBeamtimeInformation(BaseModel):
    experiment_type: JsonExperimentType
    beamtime: JsonBeamtimeOutput


class JsonReadNewAnalysisOutput(BaseModel):
    searchable_attributi: list[JsonAttributo]
    attributi: list[JsonAttributo]
    chemical_id_to_name: list[JsonChemicalIdAndName]
    experiment_types: list[JsonExperimentTypeWithBeamtimeInformation]
    filtered_data_sets: list[JsonDataSet]
    data_set_statistics: list[JsonDataSetStatistics]
    attributi_values: list[JsonAttributoValue]


class JsonReadSingleMergeResult(BaseModel):
    experiment_type: JsonExperimentType
    result: JsonMergeResult


class JsonCreateLiveStreamSnapshotOutput(BaseModel):
    id: int
    file_name: str
    description: str
    type_: str
    size_in_bytes: int
    original_path: None | str = None


# In principle, we should have one structure: indexing result. Now we
# have two, one for the "analysis" part (with FOMs), and one for the
# "job" part (with job ID etc.). We have to refactor this at some point.
class JsonIndexingJob(BaseModel):
    id: int
    job_id: None | int = None
    job_status: DBJobStatus
    started: None | int = None
    started_local: None | int = None
    stopped: None | int = None
    stopped_local: None | int = None
    is_online: bool
    stream_file: None | str = None
    source: str
    cell_description: None | str = None
    geometry_id: None | int = None
    generated_geometry_id: None | int = None
    command_line: str
    run_internal_id: int
    run_external_id: int
    beamtime: JsonBeamtimeOutput
    input_file_globs: list[str]


class JsonReadIndexingResultsOutput(BaseModel):
    indexing_jobs: list[JsonIndexingJob]


class JsonMergeJob(BaseModel):
    id: int
    job_id: None | int = None
    job_status: DBJobStatus
    parameters: JsonMergeParameters
    indexing_results: list[JsonIndexingJob]
    files_from_indexing: list[JsonFileOutput]
    point_group: str
    cell_description: str


class JsonReadMergeResultsOutput(BaseModel):
    merge_jobs: list[JsonMergeJob]


class JsonCopyExperimentTypesInput(BaseModel):
    from_beamtime: int
    to_beamtime: int


class JsonCopyExperimentTypesOutput(BaseModel):
    to_beamtime_experiment_type_ids: list[int]


class JsonDeleteRunOutput(BaseModel):
    result: bool
