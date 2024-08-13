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
    start: int
    end: int


class JsonBeamtimeOutput(BaseModel):
    id: int


class JsonBeamtime(BaseModel):
    id: BeamtimeId
    external_id: str
    proposal: str
    beamline: str
    title: str
    comment: str
    start: int
    end: int
    chemical_names: list[str]


class JsonReadBeamtime(BaseModel):
    beamtimes: list[JsonBeamtime]


class JsonEventInput(BaseModel):
    source: str
    text: str
    level: str
    fileIds: list[int]


class JsonFileOutput(BaseModel):
    id: int
    description: str
    type_: str
    original_path: None | str
    file_name: str
    size_in_bytes: int


class JsonEvent(BaseModel):
    id: int
    source: str
    text: str
    created: int
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
    stopped: int
    error: str
    latest_log: str


class JsonMergeResultStateRunning(BaseModel):
    started: int
    job_id: int
    latest_log: str


class JsonMergeResultStateDone(BaseModel):
    started: int
    stopped: int
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
    # Same as point_group comment above.
    cell_description: str
    negative_handling: None | MergeNegativeHandling
    merge_model: MergeModel
    scale_intensities: ScaleIntensities
    post_refinement: bool
    iterations: int
    polarisation: None | JsonPolarisation
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


class JsonMergeResult(BaseModel):
    id: int
    created: int
    runs: list[str]
    indexing_result_ids: list[int]
    state_queued: None | JsonMergeResultStateQueued
    state_error: None | JsonMergeResultStateError
    state_running: None | JsonMergeResultStateRunning
    state_done: None | JsonMergeResultStateDone
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


class JsonIndexingParameters(BaseModel):
    id: None | int
    cell_description: None | str
    is_online: bool
    command_line: str
    geometry_file: str


class JsonUpdateOnlineIndexingParametersInput(BaseModel):
    command_line: str
    geometry_file: str
    source: str


class JsonUpdateOnlineIndexingParametersOutput(BaseModel):
    success: bool


class JsonIndexingResult(BaseModel):
    id: int
    created: int
    started: None | int
    stopped: None | int
    parameters: JsonIndexingParameters
    program_version: str
    run_internal_id: int
    run_external_id: int
    frames: int
    hits: int
    indexed_frames: int
    indexed_crystals: int
    status: DBJobStatus
    detector_shift_x_mm: None | float
    detector_shift_y_mm: None | float
    geometry_file: str
    geometry_hash: str
    generated_geometry_file: str
    unit_cell_histograms_file_id: None | int
    has_error: bool
    # Commented out, we retrieve the log separately
    # latest_log: str


class JsonCreateIndexingForDataSetInput(BaseModel):
    data_set_id: int
    is_online: bool
    cell_description: str
    geometry_file: str
    command_line: str
    source: str


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
    detector_shift_x_mm: None | float
    detector_shift_y_mm: None | float
    geometry_file: str
    geometry_hash: str
    generated_geometry_file: str
    unit_cell_histograms_id: None | int

    # None, in this case, means "don't change/append to the log"
    latest_log: None | str


class JsonIndexingResultFinishWithError(BaseModel):
    error_message: str
    latest_log: str
    # The job might fail even before it gets to the workload manager (Slurm), in which case
    # we have no job ID.
    workload_manager_job_id: None | int


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


class JsonCreateOrUpdateRun(BaseModel):
    beamtime_id: BeamtimeId
    attributi: list[JsonAttributoValue]
    files: list[str]
    started: None | int = None
    stopped: None | int = None


class JsonCreateOrUpdateRunOutput(BaseModel):
    run_created: bool
    indexing_result_id: None | int
    error_message: None | str
    run_internal_id: None | RunInternalId


class JsonUpdateRun(BaseModel):
    id: RunInternalId
    experiment_type_id: int
    attributi: list[JsonAttributoValue]


class JsonUpdateRunOutput(BaseModel):
    result: bool


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
    new_experiment_type_id: None | int


class JsonUpdateRunsBulkOutput(BaseModel):
    result: bool


class JsonAnalysisRun(BaseModel):
    id: int
    external_id: int
    attributi: list[JsonAttributoValue]


class JsonIndexingFom(BaseModel):
    hit_rate: float
    indexing_rate: float
    indexed_frames: int
    detector_shift_x_mm: None | float
    detector_shift_y_mm: None | float


class JsonIndexingResultStillRunning(BaseModel):
    workload_manager_job_id: int
    stream_file: str
    frames: int
    hits: int
    indexed_frames: int
    indexed_crystals: int
    detector_shift_x_mm: None | float
    detector_shift_y_mm: None | float
    geometry_file: str
    geometry_hash: str
    # can be missing, in case we don't have that information but still want to signal progress
    job_started: None | int
    # None, in this case, means "don't change/append to the log"
    latest_log: None | str


class JsonIndexingStatistic(BaseModel):
    time: int
    frames: int
    hits: int
    indexed: int
    crystals: int


class JsonRunAnalysisIndexingResult(BaseModel):
    run_id: int
    foms: list[JsonIndexingFom]
    indexing_statistics: list[JsonIndexingStatistic]


class JsonDetectorShift(BaseModel):
    run_external_id: int
    shift_x_mm: float
    shift_y_mm: float


class JsonReadBeamtimeGeometryDetails(BaseModel):
    detector_shifts: list[JsonDetectorShift]


class JsonRunId(BaseModel):
    internal_run_id: int
    external_run_id: int


class JsonReadRunAnalysis(BaseModel):
    chemicals: list[JsonChemical]
    attributi: list[JsonAttributo]
    run: None | JsonAnalysisRun
    run_ids: list[JsonRunId]
    indexing_results: list[JsonRunAnalysisIndexingResult]


class JsonDataSet(BaseModel):
    id: int
    experiment_type_id: int
    attributi: list[JsonAttributoValue]


class JsonRun(BaseModel):
    id: int
    external_id: int
    attributi: list[JsonAttributoValue]
    started: int
    stopped: None | int
    files: list[JsonFileOutput]
    summary: JsonIndexingFom
    experiment_type_id: int
    data_set_ids: list[int]
    running_indexing_jobs: list[int]


class JsonUserConfig(BaseModel):
    online_crystfel: bool
    auto_pilot: bool
    current_experiment_type_id: None | int
    current_online_indexing_parameters_id: None | int


class JsonLiveStream(BaseModel):
    file_id: int
    modified: int


class JsonDataSetWithFom(BaseModel):
    data_set: JsonDataSet
    fom: JsonIndexingFom


class JsonReadRuns(BaseModel):
    live_stream: None | JsonLiveStream
    filter_dates: list[str]
    runs: list[JsonRun]
    attributi: list[JsonAttributo]
    latest_indexing_result: None | JsonRunAnalysisIndexingResult
    experiment_types: list[JsonExperimentType]
    data_sets_with_fom: list[JsonDataSetWithFom]
    events: list[JsonEvent]
    chemicals: list[JsonChemical]
    user_config: JsonUserConfig
    current_beamtime_user: None | str


class JsonCreateFileOutput(BaseModel):
    id: int
    file_name: str
    description: str
    type_: str
    size_in_bytes: int
    original_path: None | str


class JsonUserConfigurationSingleOutput(BaseModel):
    value_bool: None | bool
    value_int: None | int


class JsonCreateExperimentTypeInput(BaseModel):
    name: str
    beamtime_id: BeamtimeId
    attributi: list[JsonAttributiIdAndRole]


class JsonCreateExperimentTypeOutput(BaseModel):
    id: int


class JsonChangeRunExperimentType(BaseModel):
    run_internal_id: int
    experiment_type_id: None | int


class JsonChangeRunExperimentTypeOutput(BaseModel):
    result: bool


class JsonExperimentTypeAndRuns(BaseModel):
    id: int
    runs: list[str]


class JsonReadExperimentTypes(BaseModel):
    experiment_types: list[JsonExperimentType]
    attributi: list[JsonAttributo]
    experiment_type_id_to_run: list[JsonExperimentTypeAndRuns]


class JsonDeleteExperimentType(BaseModel):
    id: int


class JsonDeleteExperimentTypeOutput(BaseModel):
    result: bool


class JsonBeamtimeScheduleRow(BaseModel):
    users: str
    date: str
    shift: str
    comment: str
    td_support: str
    chemicals: list[int]


class JsonBeamtimeSchedule(BaseModel):
    schedule: list[JsonBeamtimeScheduleRow]


class JsonUpdateBeamtimeScheduleInput(BaseModel):
    beamtime_id: BeamtimeId
    schedule: list[JsonBeamtimeScheduleRow]


class JsonBeamtimeScheduleOutput(BaseModel):
    schedule: list[JsonBeamtimeScheduleRow]


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
    ignoreUnits: bool


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
    error: None | str
    normalized: None | str


class JsonIndexingParametersWithResults(BaseModel):
    parameters: JsonIndexingParameters
    indexing_results: list[JsonIndexingResult]
    merge_results: list[JsonMergeResult]


class JsonDataSetWithIndexingResults(BaseModel):
    data_set: JsonDataSet
    # To start indexing jobs
    internal_run_ids: list[int]
    # For display
    runs: list[str]
    indexing_results: list[JsonIndexingParametersWithResults]


class JsonReadAnalysisResults(BaseModel):
    attributi: list[JsonAttributo]
    chemical_id_to_name: list[JsonChemicalIdAndName]
    experiment_type: JsonExperimentType
    data_sets: list[JsonDataSetWithIndexingResults]


class JsonCreateLiveStreamSnapshotOutput(BaseModel):
    id: int
    file_name: str
    description: str
    type_: str
    size_in_bytes: int
    original_path: None | str


# In principle, we should have one structure: indexing result. Now we
# have two, one for the "analysis" part (with FOMs), and one for the
# "job" part (with job ID etc.). We have to refactor this at some point.
class JsonIndexingJob(BaseModel):
    id: int
    job_id: None | int
    job_status: DBJobStatus
    started: None | int
    stopped: None | int
    is_online: bool
    stream_file: None | str
    source: str
    cell_description: None | str
    geometry_file_input: str
    geometry_file_output: str
    command_line: str
    run_internal_id: int
    run_external_id: int
    beamtime: JsonBeamtime
    input_file_globs: list[str]


class JsonReadIndexingResultsOutput(BaseModel):
    indexing_jobs: list[JsonIndexingJob]


class JsonMergeJob(BaseModel):
    id: int
    job_id: None | int
    job_status: DBJobStatus
    parameters: JsonMergeParameters
    indexing_results: list[JsonIndexingJob]
    files_from_indexing: list[JsonFileOutput]
    point_group: str
    cell_description: str


class JsonReadMergeResultsOutput(BaseModel):
    merge_jobs: list[JsonMergeJob]
