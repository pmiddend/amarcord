from pydantic import BaseModel


class JsonMergeResultOuterShell(BaseModel):
    resolution: float
    ccstar: float
    r_split: float
    cc: float
    unique_reflections: int
    completeness: float
    redundancy: float
    snr: float
    min_res: float
    max_res: float


class JsonMergeResultFom(BaseModel):
    snr: float
    wilson: float | None = None
    ln_k: float | None = None
    discarded_reflections: int
    one_over_d_from: float
    one_over_d_to: float
    redundancy: float
    completeness: float
    measurements_total: int
    reflections_total: int
    reflections_possible: int
    r_split: float
    r1i: float
    r2: float
    cc: float
    ccstar: float
    ccano: float | None = None
    crdano: float | None = None
    rano: float | None = None
    rano_over_r_split: float | None = None
    d1sig: float
    d2sig: float
    outer_shell: JsonMergeResultOuterShell


class JsonMergeResultShell(BaseModel):
    one_over_d_centre: float
    nref: int
    d_over_a: float
    min_res: float
    max_res: float
    cc: float
    ccstar: float
    r_split: float
    reflections_possible: int
    completeness: float
    measurements: int
    redundancy: float
    snr: float
    mean_i: float


class JsonRefinementResultInternal(BaseModel):
    id: int | None = None
    pdb_file_id: int
    mtz_file_id: int
    r_free: float
    r_work: float
    rms_bond_angle: float
    rms_bond_length: float


class JsonMergeResultInternal(BaseModel):
    dataset: str
    mtz_file_id: int
    fom: JsonMergeResultFom
    ambigator_fg_graph_file_id: int | None = None
    detailed_foms: list[JsonMergeResultShell]
    refinement_results: list[JsonRefinementResultInternal]


class JsonMergeJobFinishedInput(BaseModel):
    latest_log: str | None = None
    error: str | None = None
    results: list[JsonMergeResultInternal]


class JsonMergeJobStartedInput(BaseModel):
    job_id: int
    time: int


class JsonMergeJobStartedOutput(BaseModel):
    time: int
    time_local: int
