import datetime
import os
import uuid
from dataclasses import replace
from io import BytesIO
from pathlib import Path
from statistics import mean
from tempfile import NamedTemporaryFile
from typing import Annotated
from typing import Any
from typing import AsyncGenerator
from typing import Callable
from typing import Final
from typing import Generator
from typing import Iterable
from typing import cast
from zipfile import ZipFile

import structlog
from fastapi import Depends
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Request
from fastapi import Response
from fastapi import UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.params import Form
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pint import UnitRegistry
from pydantic import BaseModel
from pydantic import Field
from sqlalchemy import Connection

from amarcord.amici.crystfel.merge_daemon import JsonMergeResultRootJson
from amarcord.amici.crystfel.util import CrystFELCellFile
from amarcord.amici.crystfel.util import coparse_cell_description
from amarcord.amici.crystfel.util import parse_cell_description
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import ATTRIBUTO_GROUP_MANUAL
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.asyncdb import MergeResultError
from amarcord.db.asyncdb import NotFoundError
from amarcord.db.asyncdb import live_stream_image_name
from amarcord.db.attributi import AttributoConversionFlags
from amarcord.db.attributi import attributo_sort_key
from amarcord.db.attributi import attributo_type_to_schema
from amarcord.db.attributi import datetime_from_attributo_int
from amarcord.db.attributi import datetime_to_attributo_int
from amarcord.db.attributi import schema_to_attributo_type
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.attributi_map import run_matches_dataset
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_name_and_role import AttributoIdAndRole
from amarcord.db.attributo_type import AttributoTypeBoolean
from amarcord.db.attributo_value import AttributoValue
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.chemical_type import ChemicalType
from amarcord.db.data_set import DBDataSet
from amarcord.db.db_merge_result import DBMergeResultInput
from amarcord.db.db_merge_result import DBMergeResultOutput
from amarcord.db.db_merge_result import DBMergeRuntimeStatusDone
from amarcord.db.db_merge_result import DBMergeRuntimeStatusError
from amarcord.db.db_merge_result import DBMergeRuntimeStatusRunning
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.excel_export import create_workbook
from amarcord.db.experiment_type import DBExperimentType
from amarcord.db.indexing_result import DBIndexingFOM
from amarcord.db.indexing_result import DBIndexingResultDone
from amarcord.db.indexing_result import DBIndexingResultInput
from amarcord.db.indexing_result import DBIndexingResultOutput
from amarcord.db.indexing_result import DBIndexingResultRunning
from amarcord.db.indexing_result import DBIndexingResultRuntimeStatus
from amarcord.db.indexing_result import DBIndexingResultStatistic
from amarcord.db.indexing_result import empty_indexing_fom
from amarcord.db.ingest_attributi_from_json import ingest_run_attributi_schema
from amarcord.db.merge_model import MergeModel
from amarcord.db.merge_negative_handling import MergeNegativeHandling
from amarcord.db.merge_parameters import DBMergeParameters
from amarcord.db.merge_result import MergeResult
from amarcord.db.polarisation import Polarisation
from amarcord.db.refinement_result import DBRefinementResultOutput
from amarcord.db.run_external_id import RunExternalId
from amarcord.db.run_internal_id import RunInternalId
from amarcord.db.scale_intensities import ScaleIntensities
from amarcord.db.schedule_entry import BeamtimeScheduleEntry
from amarcord.db.table_classes import BeamtimeInput
from amarcord.db.table_classes import BeamtimeOutput
from amarcord.db.table_classes import DBChemical
from amarcord.db.table_classes import DBEvent
from amarcord.db.table_classes import DBFile
from amarcord.db.table_classes import DBRunOutput
from amarcord.db.tables import create_tables_from_metadata
from amarcord.db.user_configuration import UserConfiguration
from amarcord.filter_expression import FilterInput
from amarcord.filter_expression import FilterParseError
from amarcord.filter_expression import compile_run_filter
from amarcord.json_schema import JSONSchemaArray
from amarcord.json_schema import JSONSchemaBoolean
from amarcord.json_schema import JSONSchemaInteger
from amarcord.json_schema import JSONSchemaNumber
from amarcord.json_schema import JSONSchemaString
from amarcord.json_schema import JSONSchemaUnion
from amarcord.logging_util import setup_structlog
from amarcord.util import create_intervals
from amarcord.util import group_by

setup_structlog()

logger = structlog.stdlib.get_logger(__name__)

_UNIT_REGISTRY = UnitRegistry()
ELVEFLOW_OB1_MAX_NUMBER_OF_CHANNELS: Final = 4
USER_CONFIGURATION_AUTO_PILOT: Final = "auto-pilot"
USER_CONFIGURATION_ONLINE_CRYSTFEL: Final = "online-crystfel"
USER_CONFIGURATION_CURRENT_EXPERIMENT_TYPE_ID: Final = "current-experiment-type-id"
KNOWN_USER_CONFIGURATION_VALUES: Final = [
    USER_CONFIGURATION_AUTO_PILOT,
    USER_CONFIGURATION_ONLINE_CRYSTFEL,
    USER_CONFIGURATION_CURRENT_EXPERIMENT_TYPE_ID,
]
DATE_FORMAT: Final = "%Y-%m-%d"
AUTOMATIC_ATTRIBUTI_GROUP: Final = "automatic"

hardcoded_static_folder: None | str = None

app = FastAPI(title="AMARCORD OpenAPI interface", version="1.0")
origins = [
    "http://localhost:5001",
    "http://localhost:8001",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Dependency
async def get_db() -> AsyncGenerator[AsyncDB, None]:
    db_url: Any = os.environ["DB_URL"]
    assert isinstance(db_url, str)
    echo: Any = False
    assert isinstance(echo, bool)
    context = AsyncDBContext(connection_url=db_url, echo=echo)
    # pylint: disable=assigning-non-slot
    instance = AsyncDB(context, create_tables_from_metadata(context.metadata))
    try:
        yield instance
    finally:
        await instance.dispose()


def run_has_started_date(run: DBRunOutput, date_filter: str) -> bool:
    return run.started.strftime(DATE_FORMAT) == date_filter


def format_run_id_intervals(run_ids: Iterable[int]) -> list[str]:
    return [
        str(t[0]) if t[0] == t[1] else f"{t[0]}-{t[1]}"
        for t in create_intervals(list(run_ids))
    ]


class JsonUpdateBeamtimeInput(BaseModel):
    # For creating beamtimes, we set id=0. For updates, the actual ID.
    id: int
    external_id: str
    beamline: str
    proposal: str
    title: str
    comment: str
    start: int
    end: int


class JsonBeamtimeOutput(BaseModel):
    id: int


@app.post("/api/beamtimes", tags=["beamtimes"])
async def create_beamtime(
    input_: JsonUpdateBeamtimeInput, db: AsyncDB = Depends(get_db)
) -> JsonBeamtimeOutput:
    async with db.begin() as conn:
        beamtime_id = await db.create_beamtime(
            conn,
            BeamtimeInput(
                external_id=input_.external_id,
                beamline=input_.beamline,
                proposal=input_.proposal,
                title=input_.title,
                comment=input_.comment,
                start=datetime_from_attributo_int(input_.start),
                end=datetime_from_attributo_int(input_.end),
            ),
        )
        return JsonBeamtimeOutput(id=beamtime_id)


@app.patch("/api/beamtimes", tags=["beamtimes"])
async def update_beamtime(
    input_: JsonUpdateBeamtimeInput, db: AsyncDB = Depends(get_db)
) -> JsonBeamtimeOutput:
    async with db.begin() as conn:
        await db.update_beamtime(
            conn,
            beamtime_id=BeamtimeId(input_.id),
            external_id=input_.external_id,
            beamline=input_.beamline,
            proposal=input_.proposal,
            title=input_.title,
            comment=input_.comment,
            start=datetime_from_attributo_int(input_.start),
            end=datetime_from_attributo_int(input_.end),
        )
        return JsonBeamtimeOutput(id=input_.id)


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
    beamtime_id: int
    event: JsonEventInput
    with_live_stream: bool


class JsonEventTopLevelOutput(BaseModel):
    id: int


@app.post("/api/events", tags=["events"])
async def create_event(
    input_: JsonEventTopLevelInput, db: AsyncDB = Depends(get_db)
) -> JsonEventTopLevelOutput:
    beamtime_id = BeamtimeId(input_.beamtime_id)

    async with db.begin() as conn:
        event = input_.event

        event_id = await db.create_event(
            conn,
            beamtime_id=beamtime_id,
            level=EventLogLevel(event.level),
            source=event.source,
            text=event.text,
        )
        file_ids = event.fileIds
        for file_id in file_ids:
            await db.add_file_to_event(conn, file_id, event_id)
        if input_.with_live_stream:
            existing_live_stream = await db.retrieve_file_id_by_name(
                conn, live_stream_image_name(beamtime_id)
            )
            if existing_live_stream is not None:
                # copy it, because the live stream is updated in-place
                new_file_name = live_stream_image_name(beamtime_id) + "-copy"
                duplicated_live_stream = await db.duplicate_file(
                    conn, existing_live_stream, new_file_name
                )
                await db.add_file_to_event(conn, duplicated_live_stream.id, event_id)
        return JsonEventTopLevelOutput(id=event_id)


# See https://stackoverflow.com/questions/55873174/how-do-i-return-an-image-in-fastapi
@app.get(
    "/api/{beamtimeId}/spreadsheet.zip",
    responses={200: {"content": {"application/zip": {}}}},
    include_in_schema=False,
)
async def download_spreadsheet(
    beamtimeId: BeamtimeId, db: AsyncDB = Depends(get_db)
) -> Response:
    async with db.read_only_connection() as conn:
        workbook_output = await create_workbook(db, conn, beamtimeId, with_events=True)
        workbook = workbook_output.workbook
        workbook_bytes = BytesIO()
        workbook.save(workbook_bytes)
        zipfile_bytes = BytesIO()
        with ZipFile(zipfile_bytes, "w") as result_zip:
            dirname = "amarcord-output-" + datetime.datetime.utcnow().strftime(
                "%Y-%m-%d_%H-%M-%S"
            )
            result_zip.writestr(f"{dirname}/tables.xlsx", workbook_bytes.getvalue())
            for file_id in workbook_output.files:
                file_ = await db.retrieve_file(conn, file_id, with_contents=True)
                result_zip.writestr(
                    f"{dirname}/files/{file_id}" + Path(file_.file_name).suffix,
                    cast(bytes, file_.contents),
                )
        zipfile_bytes.seek(0)

        def iterzipfile() -> Generator[bytes, None, None]:
            yield from zipfile_bytes

        return StreamingResponse(
            iterzipfile(),
            media_type="application/zip",
            headers={"Content-Disposition": f'attachment; filename="{dirname}.zip"'},
        )


class JsonAttributoValue(BaseModel):
    attributo_id: int
    # These types mirror the types of "AttributoValue"
    attributo_value_str: None | str = None
    attributo_value_int: None | int = None
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
                                else None
                            )
                        )
                    )
                )
            )
        )


class JsonChemicalWithoutId(BaseModel):
    beamtime_id: int
    name: str
    responsible_person: str
    chemical_type: ChemicalType
    attributi: list[JsonAttributoValue]
    file_ids: list[int]


class JsonChemicalWithId(BaseModel):
    id: int
    beamtime_id: int
    name: str
    responsible_person: str
    chemical_type: ChemicalType
    attributi: list[JsonAttributoValue]
    file_ids: list[int]


class JsonCreateChemicalOutput(BaseModel):
    id: int


def _json_attributo_list_to_attributi_map(
    attributi: list[DBAttributo], values: list[JsonAttributoValue]
) -> AttributiMap:
    return AttributiMap.from_types_and_raw(
        attributi,
        {a.attributo_id: a.to_attributo_value() for a in values},
    )


@app.post("/api/chemicals", tags=["chemicals"])
async def create_chemical(
    input_: JsonChemicalWithoutId, db: AsyncDB = Depends(get_db)
) -> JsonCreateChemicalOutput:
    async with db.begin() as conn:
        beamtime_id = BeamtimeId(input_.beamtime_id)

        chemical_id = await db.create_chemical(
            conn,
            beamtime_id=beamtime_id,
            name=input_.name,
            responsible_person=input_.responsible_person,
            type_=input_.chemical_type,
            attributi=AttributiMap.from_types_and_raw(
                await db.retrieve_attributi(
                    conn, beamtime_id, AssociatedTable.CHEMICAL
                ),
                {a.attributo_id: a.to_attributo_value() for a in input_.attributi},
            ),
        )
        file_ids = input_.file_ids
        for file_id in file_ids:
            await db.add_file_to_chemical(conn, file_id, chemical_id)

    return JsonCreateChemicalOutput(id=chemical_id)


@app.patch("/api/chemicals", tags=["chemicals"])
async def update_chemical(
    input_: JsonChemicalWithId, db: AsyncDB = Depends(get_db)
) -> JsonCreateChemicalOutput:
    beamtime_id = BeamtimeId(input_.beamtime_id)

    async with db.begin() as conn:
        attributi = await db.retrieve_attributi(
            conn,
            beamtime_id,
            AssociatedTable.CHEMICAL,
        )
        chemical = await db.retrieve_chemical(conn, input_.id, attributi)
        assert chemical is not None
        chemical_attributi = chemical.attributi
        chemical_attributi.extend_with_attributi_map(
            _json_attributo_list_to_attributi_map(attributi, input_.attributi)
        )
        await db.update_chemical(
            conn=conn,
            id_=input_.id,
            type_=input_.chemical_type,
            name=input_.name,
            responsible_person=input_.responsible_person,
            attributi=chemical_attributi,
        )
        await db.remove_files_from_chemical(conn, input_.id)
        file_ids = input_.file_ids
        for file_id in file_ids:
            await db.add_file_to_chemical(conn, file_id, input_.id)

    return JsonCreateChemicalOutput(id=input_.id)


class JsonDeleteChemicalInput(BaseModel):
    id: int


class JsonDeleteChemicalOutput(BaseModel):
    id: int


@app.delete("/api/chemicals", tags=["chemicals"])
async def delete_chemical(
    input_: JsonDeleteChemicalInput, db: AsyncDB = Depends(get_db)
) -> JsonDeleteChemicalOutput:
    async with db.begin() as conn:
        await db.delete_chemical(
            conn,
            id_=input_.id,
            delete_in_dependencies=True,
        )

    return JsonDeleteChemicalOutput(id=input_.id)


def _encode_file_output(f: DBFile) -> JsonFileOutput:
    assert f.id is not None
    return JsonFileOutput(
        id=f.id,
        description=f.description,
        type_=f.type_,
        file_name=f.file_name,
        size_in_bytes=f.size_in_bytes,
        original_path=f.original_path,
    )


class JsonUserConfig(BaseModel):
    online_crystfel: bool
    auto_pilot: bool
    current_experiment_type_id: None | int


def _encode_user_configuration(c: UserConfiguration) -> JsonUserConfig:
    return JsonUserConfig(
        online_crystfel=c.use_online_crystfel,
        auto_pilot=c.auto_pilot,
        current_experiment_type_id=c.current_experiment_type_id,
    )


class JsonChemical(BaseModel):
    id: int
    beamtime_id: int
    name: str
    responsible_person: str
    chemical_type: ChemicalType
    attributi: list[JsonAttributoValue]
    files: list[JsonFileOutput]


def _encode_attributo_value(
    attributo_id: int, attributo_value: AttributoValue
) -> JsonAttributoValue:
    return JsonAttributoValue(
        attributo_id=attributo_id,
        attributo_value_str=(
            attributo_value if isinstance(attributo_value, str) else None
        ),
        attributo_value_int=(
            attributo_value if isinstance(attributo_value, int) else None
        ),
        attributo_value_float=(
            attributo_value if isinstance(attributo_value, float) else None
        ),
        attributo_value_bool=(
            attributo_value if isinstance(attributo_value, bool) else None
        ),
        # we cannot thoroughly test the array for type-correctness (or we dont' want to, rather)
        attributo_value_list_str=(
            attributo_value
            if isinstance(attributo_value, list)
            and (not attributo_value or isinstance(attributo_value[0], str))
            else None
        ),  # pyright: ignore[reportGeneralTypeIssues]
        attributo_value_list_float=(
            attributo_value
            if isinstance(attributo_value, list)
            and (not attributo_value or isinstance(attributo_value[0], (int, float)))
            else None
        ),  # pyright: ignore[reportGeneralTypeIssues]
        attributo_value_list_bool=(
            attributo_value
            if isinstance(attributo_value, list)
            and (not attributo_value or isinstance(attributo_value[0], bool))
            else None
        ),  # pyright: ignore[reportGeneralTypeIssues]
    )


def _encode_attributi_map(m: AttributiMap) -> list[JsonAttributoValue]:
    return [
        _encode_attributo_value(int(attributo_id), attributo_value)
        for attributo_id, attributo_value in m.items()
    ]


def _encode_chemical(a: DBChemical) -> JsonChemical:
    return JsonChemical(
        id=a.id,
        beamtime_id=a.beamtime_id,
        name=a.name,
        responsible_person=a.responsible_person,
        chemical_type=a.type_,
        attributi=_encode_attributi_map(a.attributi),
        files=[_encode_file_output(f) for f in a.files],
    )


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
    result: MergeResult


class JsonPolarisation(BaseModel):
    angle: int
    percent: int


class JsonMergeParameters(BaseModel):
    point_group: str
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
    cell_description: str
    point_group: str
    state_queued: None | JsonMergeResultStateQueued
    state_error: None | JsonMergeResultStateError
    state_running: None | JsonMergeResultStateRunning
    state_done: None | JsonMergeResultStateDone
    parameters: JsonMergeParameters
    refinement_results: list[JsonRefinementResult]


def _encode_merge_result(
    mr: DBMergeResultOutput,
    refinement_results: list[DBRefinementResultOutput],
    run_id_formatter: None | Callable[[RunInternalId], int] = None,
) -> JsonMergeResult:
    return JsonMergeResult(
        id=mr.id,
        created=datetime_to_attributo_int(mr.created),
        # We don't export the indexing results here yet. No clear reason other than laziness
        runs=format_run_id_intervals(
            run_id_formatter(ir.run_id) if run_id_formatter is not None else ir.run_id
            for ir in mr.indexing_results
        ),
        cell_description=coparse_cell_description(mr.parameters.cell_description),
        point_group=mr.parameters.point_group,
        state_queued=(
            JsonMergeResultStateQueued(queued=True)
            if mr.runtime_status is None
            else None
        ),
        state_running=(
            JsonMergeResultStateRunning(
                started=datetime_to_attributo_int(mr.runtime_status.started),
                job_id=mr.runtime_status.job_id,
                latest_log=mr.runtime_status.recent_log,
            )
            if isinstance(mr.runtime_status, DBMergeRuntimeStatusRunning)
            else None
        ),
        state_error=(
            JsonMergeResultStateError(
                started=datetime_to_attributo_int(mr.runtime_status.started),
                stopped=datetime_to_attributo_int(mr.runtime_status.stopped),
                error=mr.runtime_status.error,
                latest_log=mr.runtime_status.recent_log,
            )
            if isinstance(mr.runtime_status, DBMergeRuntimeStatusError)
            else None
        ),
        state_done=(
            JsonMergeResultStateDone(
                started=datetime_to_attributo_int(mr.runtime_status.started),
                stopped=datetime_to_attributo_int(mr.runtime_status.stopped),
                result=mr.runtime_status.result,
            )
            if isinstance(mr.runtime_status, DBMergeRuntimeStatusDone)
            else None
        ),
        parameters=JsonMergeParameters(
            point_group=mr.parameters.point_group,
            cell_description=coparse_cell_description(mr.parameters.cell_description),
            negative_handling=mr.parameters.negative_handling,
            merge_model=mr.parameters.merge_model,
            scale_intensities=mr.parameters.scale_intensities,
            post_refinement=mr.parameters.post_refinement,
            iterations=mr.parameters.iterations,
            polarisation=(
                JsonPolarisation(
                    angle=int(mr.parameters.polarisation.angle.m),
                    percent=mr.parameters.polarisation.percentage,
                )
                if mr.parameters.polarisation is not None
                else None
            ),
            start_after=mr.parameters.start_after,
            stop_after=mr.parameters.stop_after,
            rel_b=mr.parameters.rel_b,
            no_pr=mr.parameters.no_pr,
            force_bandwidth=mr.parameters.force_bandwidth,
            force_radius=mr.parameters.force_radius,
            force_lambda=mr.parameters.force_lambda,
            no_delta_cc_half=mr.parameters.no_delta_cc_half,
            max_adu=mr.parameters.max_adu,
            min_measurements=mr.parameters.min_measurements,
            logs=mr.parameters.logs,
            min_res=mr.parameters.min_res,
            push_res=mr.parameters.push_res,
            w=mr.parameters.w,
        ),
        refinement_results=[
            JsonRefinementResult(
                id=rr.id,
                merge_result_id=rr.merge_result_id,
                pdb_file_id=rr.pdb_file_id,
                mtz_file_id=rr.mtz_file_id,
                r_free=rr.r_free,
                r_work=rr.r_work,
                rms_bond_angle=rr.rms_bond_angle,
                rms_bond_length=rr.rms_bond_length,
            )
            for rr in refinement_results
        ],
    )


class JsonBeamtime(BaseModel):
    id: int
    external_id: str
    proposal: str
    beamline: str
    title: str
    comment: str
    start: int
    end: int
    chemical_names: list[str]


def _encode_beamtime(bt: BeamtimeOutput) -> JsonBeamtime:
    return JsonBeamtime(
        id=bt.id,
        external_id=bt.external_id,
        proposal=bt.proposal,
        beamline=bt.beamline,
        title=bt.title,
        comment=bt.comment,
        start=datetime_to_attributo_int(bt.start),
        end=datetime_to_attributo_int(bt.end),
        chemical_names=bt.chemical_names if bt.chemical_names is not None else [],
    )


class JsonReadBeamtime(BaseModel):
    beamtimes: list[JsonBeamtime]


@app.get("/api/beamtimes", tags=["beamtimes"], response_model_exclude_defaults=True)
async def read_beamtimes(db: AsyncDB = Depends(get_db)) -> JsonReadBeamtime:
    async with db.read_only_connection() as conn:
        return JsonReadBeamtime(
            beamtimes=[_encode_beamtime(bt) for bt in await db.retrieve_beamtimes(conn)]
        )


@app.get(
    "/api/beamtimes/{beamtimeId}",
    tags=["beamtimes"],
    response_model_exclude_defaults=True,
)
async def read_beamtime(beamtimeId: int, db: AsyncDB = Depends(get_db)) -> JsonBeamtime:
    async with db.read_only_connection() as conn:
        return _encode_beamtime(
            await db.retrieve_beamtime(conn, BeamtimeId(beamtimeId))
        )


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


def _encode_attributo(a: DBAttributo) -> JsonAttributo:
    schema = attributo_type_to_schema(a.attributo_type)
    if isinstance(schema, JSONSchemaInteger):
        return JsonAttributo(
            id=a.id,
            name=a.name,
            description=a.description,
            group=a.group,
            associated_table=a.associated_table,
            attributo_type_integer=schema,
        )
    if isinstance(schema, JSONSchemaNumber):
        return JsonAttributo(
            id=a.id,
            name=a.name,
            description=a.description,
            group=a.group,
            associated_table=a.associated_table,
            attributo_type_number=schema,
        )
    if isinstance(schema, JSONSchemaString):
        return JsonAttributo(
            id=a.id,
            name=a.name,
            description=a.description,
            group=a.group,
            associated_table=a.associated_table,
            attributo_type_string=schema,
        )
    if isinstance(schema, JSONSchemaArray):
        return JsonAttributo(
            id=a.id,
            name=a.name,
            description=a.description,
            group=a.group,
            associated_table=a.associated_table,
            attributo_type_array=schema,
        )
    return JsonAttributo(
        id=a.id,
        name=a.name,
        description=a.description,
        group=a.group,
        associated_table=a.associated_table,
        attributo_type_boolean=schema,
    )


@app.get(
    "/api/chemicals/{beamtimeId}",
    tags=["chemicals"],
    response_model_exclude_defaults=True,
)
async def read_chemicals(
    beamtimeId: BeamtimeId, db: AsyncDB = Depends(get_db)
) -> JsonReadChemicals:
    async with db.begin() as conn:
        attributi = await db.retrieve_attributi(
            conn, beamtimeId, associated_table=AssociatedTable.CHEMICAL
        )
        return JsonReadChemicals(
            chemicals=[
                _encode_chemical(a)
                for a in await db.retrieve_chemicals(conn, beamtimeId, attributi)
            ],
            attributi=[_encode_attributo(a) for a in attributi],
        )


class JsonIndexingResult(BaseModel):
    frames: int
    hits: int
    indexed_frames: int
    indexed_crystals: int
    done: bool
    detector_shift_x_mm: None | float
    detector_shift_y_mm: None | float


class JsonIndexingResultRootJson(BaseModel):
    error: None | str
    result: None | JsonIndexingResult


class JsonIndexingJobUpdateOutput(BaseModel):
    result: bool


@app.post(
    "/api/indexing/{indexingResultId}",
    tags=["analysis", "processing"],
    response_model_exclude_defaults=True,
)
async def indexing_job_update(
    indexingResultId: int,
    json_result: JsonIndexingResultRootJson,
    db: AsyncDB = Depends(get_db),
) -> JsonIndexingJobUpdateOutput:
    job_logger = logger.bind(indexing_result_id=indexingResultId)
    job_logger.info("update")

    def fom_from_json_result(jr: JsonIndexingResult) -> DBIndexingFOM:
        return DBIndexingFOM(
            hit_rate=jr.hits / jr.frames * 100.0 if jr.frames != 0 else 0,
            indexing_rate=jr.indexed_frames / jr.hits * 100 if jr.hits != 0 else 0.0,
            indexed_frames=jr.indexed_frames,
            detector_shift_x_mm=jr.detector_shift_x_mm,
            detector_shift_y_mm=jr.detector_shift_y_mm,
        )

    async with db.begin() as conn:
        current_indexing_result = await db.retrieve_indexing_result(
            conn, indexingResultId
        )
        if current_indexing_result is None:
            job_logger.error(f"indexing result with ID {indexingResultId} not found")
            return JsonIndexingJobUpdateOutput(result=False)
        runtime_status = current_indexing_result.runtime_status

        final_status: None | DBIndexingResultRuntimeStatus
        if json_result.error is not None:
            final_status = (
                DBIndexingResultDone(
                    job_error=json_result.error,
                    fom=runtime_status.fom,
                    stream_file=runtime_status.stream_file,
                )
                if isinstance(runtime_status, DBIndexingResultRunning)
                else DBIndexingResultDone(
                    job_error=json_result.error,
                    fom=empty_indexing_fom,
                    # This is confusing, I know. The idea is: if we're in this "if" branch, then we have an error.
                    # This means that in theory, we can only be in state "Running". However, to be absolutely sure not to
                    # lose the "stream file" information, we take it from the runtime status, if that's not None.
                    stream_file=(
                        runtime_status.stream_file
                        if runtime_status is not None
                        else Path("dummy-after-error")
                    ),
                )
            )
        elif json_result.result is not None:
            final_status = (
                DBIndexingResultDone(
                    job_error=None,
                    fom=fom_from_json_result(json_result.result),
                    stream_file=runtime_status.stream_file,
                )
                if isinstance(runtime_status, DBIndexingResultRunning)
                else DBIndexingResultDone(
                    job_error=None,
                    fom=fom_from_json_result(json_result.result),
                    # This is confusing, I know. The idea is: if we're in this "if" branch, then we have a final result now.
                    # This means that in theory, we can only be in state "Running". However, to be absolutely sure not to
                    # lose the "stream file" information, we take it from the runtime status, if that's not None.
                    stream_file=(
                        runtime_status.stream_file
                        if runtime_status is not None
                        else Path("dummy-after-success")
                    ),
                )
            )
            await db.add_indexing_result_statistic(
                conn,
                DBIndexingResultStatistic(
                    indexing_result_id=indexingResultId,
                    time=datetime.datetime.utcnow(),
                    frames=json_result.result.frames,
                    hits=json_result.result.hits,
                    indexed_frames=json_result.result.indexed_frames,
                    indexed_crystals=json_result.result.indexed_crystals,
                ),
            )
        else:
            final_status = None

        if final_status is None:
            job_logger.error(
                "couldn't parse indexing result: both error and result are None"
            )
            return JsonIndexingJobUpdateOutput(result=False)

        await db.update_indexing_result_status(
            conn,
            indexingResultId,
            final_status,
        )
    return JsonIndexingJobUpdateOutput(result=True)


class JsonMergeJobUpdateOutput(BaseModel):
    result: bool


async def _safe_create_new_event(
    this_logger: structlog.stdlib.BoundLogger,
    db: AsyncDB,
    conn: Connection,
    beamtime_id: BeamtimeId,
    text: str,
    level: EventLogLevel,
    source: str,
) -> None:
    try:
        await db.create_event(conn, beamtime_id, level, source, text)
    except:
        this_logger.exception("error writing event log")


@app.post(
    "/api/merging/{mergeResultId}",
    tags=["merging"],
    response_model_exclude_defaults=True,
)
async def merge_job_finished(
    mergeResultId: int,
    json_merge_result: JsonMergeResultRootJson,
    db: AsyncDB = Depends(get_db),
) -> JsonMergeJobUpdateOutput:
    job_logger = logger.bind(merge_result_id=mergeResultId)

    job_logger.info("merge job has finished")

    async with db.begin() as conn:
        current_merge_result_status = next(
            iter(
                await db.retrieve_merge_results(
                    conn,
                    beamtime_id=None,
                    merge_result_id_filter=mergeResultId,
                )
            ),
            None,
        )

        if current_merge_result_status is None:
            logger.error("merge job not found in DB")
            return JsonMergeJobUpdateOutput(result=False)

        runtime_status = current_merge_result_status.runtime_status
        if not isinstance(runtime_status, DBMergeRuntimeStatusRunning):
            logger.warning(
                f"merge result status is {runtime_status}, not running; this might be fine though"
            )
            started = datetime.datetime.utcnow()
            recent_log = ""
        else:
            started = runtime_status.started
            recent_log = runtime_status.recent_log

        stopped_time = datetime.datetime.utcnow()

        job_logger.info("json request content is valid")

        if json_merge_result.error is not None:
            await _safe_create_new_event(
                job_logger,
                db,
                conn,
                await db.retrieve_beamtime_for_run(
                    conn,
                    current_merge_result_status.indexing_results[0].run_id,
                ),
                f"merge result {mergeResultId} finished with error `{json_merge_result.error}`",
                EventLogLevel.INFO,
                "API",
            )
            job_logger.error(
                f"semantic error in json content: {json_merge_result.error}"
            )
            await db.update_merge_result_status(
                conn,
                mergeResultId,
                DBMergeRuntimeStatusError(
                    error=json_merge_result.error,
                    started=started,
                    stopped=stopped_time,
                    recent_log=recent_log,
                ),
            )
            return JsonMergeJobUpdateOutput(result=False)

        assert (
            json_merge_result.result is not None
        ), f"both error and result are none in output: {json_merge_result}"

        await _safe_create_new_event(
            job_logger,
            db,
            conn,
            await db.retrieve_beamtime_for_run(
                conn,
                current_merge_result_status.indexing_results[0].run_id,
            ),
            f"merge result {mergeResultId} finished successfully",
            EventLogLevel.INFO,
            "API",
        )
        await db.update_merge_result_status(
            conn,
            mergeResultId,
            DBMergeRuntimeStatusDone(
                started=started,
                stopped=stopped_time,
                result=json_merge_result.result,
                recent_log=recent_log,
            ),
        )

        for rr in json_merge_result.result.refinement_results:
            await db.create_refinement_result(
                conn,
                mergeResultId,
                rr.pdb_file_id,
                rr.mtz_file_id,
                rfree=rr.r_free,
                rwork=rr.r_work,
                rms_bond_angle=rr.rms_bond_angle,
                rms_bond_length=rr.rms_bond_length,
            )
        return JsonMergeJobUpdateOutput(result=True)


class JsonStartMergeJobForDataSetInput(BaseModel):
    strict_mode: bool
    beamtime_id: int
    merge_model: MergeModel
    scale_intensities: ScaleIntensities
    post_refinement: bool
    iterations: int
    polarisation: None | JsonPolarisation
    negative_handling: MergeNegativeHandling
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


class JsonStartMergeJobForDataSetOutput(BaseModel):
    merge_result_id: int


@app.post(
    "/api/merging/{dataSetId}/start",
    tags=["merging"],
    response_model_exclude_defaults=True,
)
async def start_merge_job_for_data_set(
    dataSetId: int,
    input_: JsonStartMergeJobForDataSetInput,
    db: AsyncDB = Depends(get_db),
) -> JsonStartMergeJobForDataSetOutput:
    async with db.begin() as conn:
        strict_mode = input_.strict_mode
        beamtime_id = BeamtimeId(input_.beamtime_id)
        attributi = await db.retrieve_attributi(
            conn, beamtime_id, associated_table=None
        )
        data_set = await db.retrieve_data_set(conn, dataSetId, attributi)
        if data_set is None:
            raise HTTPException(
                status_code=400, detail=f'Data set with ID "{dataSetId}" not found'
            )
        runs = [
            run
            for run in await db.retrieve_runs(
                conn,
                beamtime_id,
                attributi,
            )
            if run.experiment_type_id == data_set.experiment_type_id
            and run_matches_dataset(run.attributi, data_set.attributi)
        ]
        if not runs:
            raise HTTPException(
                status_code=400, detail=f"Data set with ID {dataSetId} has no runs!"
            )
        indexing_results_by_run_id: dict[int, list[DBIndexingResultOutput]] = group_by(
            (
                ir
                for ir in await db.retrieve_indexing_results(
                    conn, beamtime_id=beamtime_id
                )
                if isinstance(ir.runtime_status, DBIndexingResultDone)
            ),
            lambda ir: ir.run_id,
        )
        chosen_indexing_results: list[DBIndexingResultOutput] = []
        cell_descriptions: set[CrystFELCellFile] = set()
        point_groups: set[str] = set()
        for run in runs:
            irs = sorted(
                indexing_results_by_run_id.get(run.id, []),
                key=lambda ir: ir.id,
                reverse=True,
            )
            if not irs:
                if strict_mode:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Run {run.id} has no indexing results and strict mode is on; cannot merge",
                    )
                continue
            chosen_result = irs[0]
            if chosen_result.cell_description is not None:
                cell_descriptions.add(chosen_result.cell_description)
            if chosen_result.point_group is not None:
                point_groups.add(chosen_result.point_group)
            chosen_indexing_results.append(chosen_result)
        if not chosen_indexing_results:
            raise HTTPException(
                status_code=400,
                detail="Found no indexing results for the runs "
                + ",".join(str(r.id) for r in runs),
            )
        # Shouldn't happen, since for each indexing result we automatically have a cell description and point group, but
        # the type system is too clunky to express that easily.
        if not cell_descriptions:
            raise HTTPException(
                status_code=400,
                detail="Found no cell descriptions for the runs "
                + ",".join(str(r.id) for r in runs),
            )
        if not point_groups:
            raise HTTPException(
                status_code=400,
                detail="Found no cell descriptions for the runs "
                + ",".join(str(r.id) for r in runs),
            )
        if len(cell_descriptions) > 1:
            raise HTTPException(
                status_code=400,
                detail="We have more than one cell description and cannot merge: "
                + ", ".join(coparse_cell_description(c) for c in cell_descriptions),
            )
        if len(point_groups) > 1:
            raise HTTPException(
                status_code=400,
                detail="We have more than one point group and cannot merge: "
                + ", ".join(f for f in point_groups),
            )
        negative_handling = input_.negative_handling
        polarisation = input_.polarisation
        merge_result_id = await db.create_merge_result(
            conn,
            DBMergeResultInput(
                created=datetime.datetime.utcnow(),
                indexing_results=chosen_indexing_results,
                parameters=DBMergeParameters(
                    point_group=next(iter(point_groups)),
                    cell_description=next(iter(cell_descriptions)),
                    negative_handling=negative_handling,
                    merge_model=MergeModel(input_.merge_model),
                    scale_intensities=ScaleIntensities(input_.scale_intensities),
                    post_refinement=input_.post_refinement,
                    iterations=input_.iterations,
                    polarisation=(
                        Polarisation(
                            polarisation.angle * _UNIT_REGISTRY.degrees,
                            percentage=polarisation.percent,
                        )
                        if polarisation is not None
                        else None
                    ),
                    start_after=input_.start_after,
                    stop_after=input_.stop_after,
                    rel_b=input_.rel_b,
                    no_pr=input_.no_pr,
                    force_bandwidth=input_.force_bandwidth,
                    force_radius=input_.force_radius,
                    force_lambda=input_.force_lambda,
                    no_delta_cc_half=input_.no_delta_cc_half,
                    max_adu=input_.max_adu,
                    min_measurements=input_.min_measurements,
                    logs=input_.logs,
                    min_res=input_.min_res,
                    push_res=input_.push_res,
                    w=input_.w,
                ),
                runtime_status=None,
            ),
        )
        if isinstance(merge_result_id, MergeResultError):
            raise Exception(str(merge_result_id))
        await _safe_create_new_event(
            logger,
            db,
            conn,
            beamtime_id,
            f"merge result {merge_result_id} started",
            EventLogLevel.INFO,
            "API",
        )
        return JsonStartMergeJobForDataSetOutput(merge_result_id=merge_result_id)


class JsonStartRunOutput(BaseModel):
    run_internal_id: int


@app.get(
    "/api/runs/{runExternalId}/start/{beamtimeId}",
    tags=["runs"],
    response_model_exclude_defaults=True,
)
async def start_run(
    runExternalId: RunExternalId,
    beamtimeId: BeamtimeId,
    db: AsyncDB = Depends(get_db),
) -> JsonStartRunOutput:
    async with db.begin() as conn:
        attributi = await db.retrieve_attributi(conn, beamtimeId, AssociatedTable.RUN)

        experiment_type_id = (
            await db.retrieve_configuration(conn, beamtime_id=beamtimeId)
        ).current_experiment_type_id
        if experiment_type_id is None:
            raise HTTPException(
                status_code=400, detail="Cannot create run, no experiment type set!"
            )
        id_ = await db.create_run(
            conn,
            run_external_id=runExternalId,
            started=datetime.datetime.utcnow(),
            attributi=attributi,
            beamtime_id=beamtimeId,
            attributi_map=AttributiMap.from_types_and_raw(
                types=attributi,
                raw_attributi={},
            ),
            experiment_type_id=experiment_type_id,
            keep_manual_attributes_from_previous_run=True,
        )
        return JsonStartRunOutput(run_internal_id=id_)


class JsonStopRunOutput(BaseModel):
    result: bool


@app.get(
    "/api/runs/stop-latest/{beamtimeId}",
    tags=["runs"],
    response_model_exclude_defaults=True,
)
async def stop_latest_run(
    beamtimeId: BeamtimeId, db: AsyncDB = Depends(get_db)
) -> JsonStopRunOutput:
    async with db.begin() as conn:
        attributi = await db.retrieve_attributi(conn, beamtimeId, AssociatedTable.RUN)

        latest_run = await db.retrieve_latest_run(conn, beamtimeId, attributi)

        if latest_run is not None:
            await db.update_run(
                conn,
                internal_id=latest_run.id,
                stopped=datetime.datetime.utcnow(),
                attributi=latest_run.attributi,
                new_experiment_type_id=None,
            )
            return JsonStopRunOutput(result=True)

        return JsonStopRunOutput(result=False)


class JsonCreateOrUpdateRun(BaseModel):
    beamtime_id: int
    attributi: list[JsonAttributoValue]
    started: None | int = None
    stopped: None | int = None


class JsonCreateOrUpdateRunOutput(BaseModel):
    run_created: bool
    indexing_result_id: None | int
    error_message: None | str
    run_internal_id: None | int


@app.post(
    "/api/runs/{runExternalId}", tags=["runs"], response_model_exclude_defaults=True
)
async def create_or_update_run(
    runExternalId: RunExternalId,
    input_: JsonCreateOrUpdateRun,
    db: AsyncDB = Depends(get_db),
) -> JsonCreateOrUpdateRunOutput:
    run_logger = logger.bind(run_external_id=runExternalId)
    run_logger.info("creating (or updating) run")

    async with db.begin() as conn:
        beamtime_id = BeamtimeId(input_.beamtime_id)
        raw_attributi = input_.attributi
        attributi = await db.retrieve_attributi(conn, beamtime_id, None)
        chemicals = await db.retrieve_chemicals(conn, beamtime_id, attributi)
        experiment_type_id = (
            await db.retrieve_configuration(conn, beamtime_id)
        ).current_experiment_type_id
        if experiment_type_id is None:
            raise HTTPException(
                status_code=400, detail="Cannot create run, no experiment type set!"
            )
        run_internal_id = await db.retrieve_run_internal_from_external_id(
            conn, runExternalId, beamtime_id
        )
        assert run_internal_id is None or isinstance(
            run_internal_id, int
        ), f"run internal id is {run_internal_id}"
        run_in_db = (
            await db.retrieve_run(conn, run_internal_id, attributi)
            if run_internal_id is not None
            else None
        )
        current_run = (
            run_in_db
            if run_in_db is not None
            else DBRunOutput(
                id=RunInternalId(-1),
                external_id=runExternalId,
                experiment_type_id=experiment_type_id,
                beamtime_id=beamtime_id,
                started=(
                    datetime.datetime.utcnow()
                    if input_.started is None
                    else datetime_from_attributo_int(input_.started)
                ),
                stopped=(
                    None
                    if input_.stopped is None
                    else datetime_from_attributo_int(input_.stopped)
                ),
                attributi=AttributiMap.from_types_and_json_dict(attributi, {}),
                files=[],
            )
        )
        current_run.attributi.extend_with_attributi_map(
            _json_attributo_list_to_attributi_map(attributi, raw_attributi)
        )
        if run_internal_id is not None:
            run_logger.info("updating run")
            await db.update_run(
                conn,
                internal_id=run_internal_id,
                stopped=(
                    current_run.stopped
                    if input_.stopped is None
                    else datetime_from_attributo_int(input_.stopped)
                ),
                attributi=current_run.attributi,
                new_experiment_type_id=experiment_type_id,
            )
        else:
            run_logger.info("creating run")
            run_internal_id = await db.create_run(
                conn,
                run_external_id=runExternalId,
                started=(
                    datetime_from_attributo_int(input_.started)
                    if input_.started is not None
                    else datetime.datetime.utcnow()
                ),
                beamtime_id=beamtime_id,
                attributi=attributi,
                attributi_map=current_run.attributi,
                experiment_type_id=experiment_type_id,
                keep_manual_attributes_from_previous_run=False,
            )

        async def _inner_create_new_event(text: str) -> None:
            await _safe_create_new_event(
                logger,
                db,
                conn,
                beamtime_id,
                f"run {runExternalId}: {text}",
                EventLogLevel.INFO,
                "API",
            )

        config = await db.retrieve_configuration(conn, beamtime_id)
        if run_in_db is not None:
            run_logger.info("CrystFEL online not needed, run is updated, not created")
            indexing_result_id = None
        elif not config.use_online_crystfel:
            run_logger.info("CrystFEL online deactivated, not creating indexing job")
            indexing_result_id = None
        else:
            run_logger.info("adding CrystFEL online job")
            point_group_attributo = next(
                iter(a for a in attributi if a.name == "point group"), None
            )
            if point_group_attributo is None:
                message = "cannot start CrystFEL online: have no point group attributo"
                await _inner_create_new_event(message)
                raise HTTPException(
                    status_code=400,
                    detail=message,
                )
            cell_description_attributo = next(
                iter(a for a in attributi if a.name == "cell description"), None
            )
            if cell_description_attributo is None:
                message = (
                    "cannot start CrystFEL online: have no cell description attributo"
                )
                await _inner_create_new_event(message)
                raise HTTPException(
                    status_code=400,
                    detail=message,
                )
            point_group: None | str = None
            cell_description_str: None | str = None
            channel_chemical_id: None | int = None
            # For indexing, we need to provide one chemical ID that serves as _the_ chemical ID for the indexing job
            # (kind of a bug right now). So, if we don't find any chemicals with cell information, we just use the first
            # one which is of type "crystal". Since it's totally valid to leave out cell information for crystals, for
            # example in the case where you actually don't know that and want to find out.
            crystal_chemicals: list[DBChemical] = []
            for channel in range(1, ELVEFLOW_OB1_MAX_NUMBER_OF_CHANNELS + 1):
                chemical_id_attributo = next(
                    iter(
                        a.id
                        for a in attributi
                        if a.name == f"channel_{channel}_chemical_id"
                    ),
                    None,
                )
                if chemical_id_attributo is None:
                    continue
                this_channel_chemical_id = current_run.attributi.select_chemical_id(
                    chemical_id_attributo
                )
                if this_channel_chemical_id is None:
                    continue
                chemical = next(
                    iter(c for c in chemicals if c.id == this_channel_chemical_id), None
                )
                if chemical is None:
                    run_logger.warning(
                        f"chemical in channel {channel} with ID {this_channel_chemical_id} not found"
                    )
                    continue
                if chemical.type_ == ChemicalType.CRYSTAL:
                    crystal_chemicals.append(chemical)
                this_point_group = chemical.attributi.select_string(
                    point_group_attributo.id
                )
                this_cell_description = chemical.attributi.select_string(
                    cell_description_attributo.id
                )
                if this_point_group is not None and this_cell_description is not None:
                    point_group = this_point_group
                    cell_description_str = this_cell_description
                    channel_chemical_id = this_channel_chemical_id
                    break

            if channel_chemical_id is None:
                if not crystal_chemicals:
                    error_message = (
                        "cannot start CrystFEL online: chemicals with cell information and none "
                        + 'of type "crystal" detected'
                    )
                    await _inner_create_new_event(error_message)
                    run_logger.warning(error_message)
                    return JsonCreateOrUpdateRunOutput(
                        run_created=False,
                        indexing_result_id=None,
                        error_message=error_message,
                        run_internal_id=None,
                    )
                chosen_chemical = crystal_chemicals[0]
                channel_chemical_id = chosen_chemical.id
                info_message = (
                    "no chemicals with cell information found, taking the first chemical of type "
                    + f' "crystal": {chosen_chemical.name} (id {chosen_chemical.id})'
                )
                await _inner_create_new_event(info_message)
                run_logger.info(info_message)

            cell_description: None | CrystFELCellFile
            if cell_description_str is not None:
                cell_description = parse_cell_description(cell_description_str)
                if cell_description is None:
                    error_message = f"cannot start indexing job, cell description is invalid: {cell_description_str}"
                    await _inner_create_new_event(error_message)
                    logger.error(error_message)
                    return JsonCreateOrUpdateRunOutput(
                        run_created=False,
                        indexing_result_id=None,
                        error_message=error_message,
                        run_internal_id=None,
                    )
            else:
                cell_description = None

            run_logger.info(
                f"creating CrystFEL online job for chemical {channel_chemical_id}"
            )
            indexing_result_id = await db.create_indexing_result(
                conn,
                DBIndexingResultInput(
                    created=datetime.datetime.utcnow(),
                    run_id=run_internal_id,
                    frames=0,
                    hits=0,
                    not_indexed_frames=0,
                    runtime_status=None,
                    point_group=(
                        point_group
                        if point_group is not None and point_group.strip()
                        else None
                    ),
                    cell_description=cell_description,
                    chemical_id=channel_chemical_id,
                ),
            )

    return JsonCreateOrUpdateRunOutput(
        run_created=run_in_db is None,
        indexing_result_id=indexing_result_id,
        error_message=None,
        run_internal_id=run_internal_id,
    )


class JsonUpdateRun(BaseModel):
    id: int
    beamtime_id: int
    experiment_type_id: int
    attributi: list[JsonAttributoValue]


class JsonUpdateRunOutput(BaseModel):
    result: bool


@app.patch("/api/runs", tags=["runs"], response_model_exclude_defaults=True)
async def update_run(
    input_: JsonUpdateRun, db: AsyncDB = Depends(get_db)
) -> JsonUpdateRunOutput:
    async with db.begin() as conn:
        run_id = RunInternalId(input_.id)
        beamtime_id = BeamtimeId(input_.beamtime_id)
        experiment_type_id = input_.experiment_type_id
        attributi = await db.retrieve_attributi(conn, beamtime_id, AssociatedTable.RUN)
        current_run = await db.retrieve_run(conn, run_id, attributi)
        raw_attributi = input_.attributi
        current_run.attributi.extend_with_attributi_map(
            _json_attributo_list_to_attributi_map(attributi, raw_attributi)
        )
        await db.update_run(
            conn,
            internal_id=run_id,
            stopped=current_run.stopped,
            attributi=current_run.attributi,
            new_experiment_type_id=experiment_type_id,
        )

    return JsonUpdateRunOutput(result=True)


def _summary_from_foms(ir: list[DBIndexingFOM]) -> DBIndexingFOM:
    if not ir:
        return empty_indexing_fom
    shifts_x = [e.detector_shift_x_mm for e in ir if e.detector_shift_x_mm is not None]
    shifts_y = [e.detector_shift_y_mm for e in ir if e.detector_shift_y_mm is not None]
    return DBIndexingFOM(
        hit_rate=mean(x.hit_rate for x in ir),
        indexing_rate=mean(x.indexing_rate for x in ir),
        indexed_frames=sum(x.indexed_frames for x in ir),
        detector_shift_x_mm=mean(shifts_x) if shifts_x else None,
        detector_shift_y_mm=mean(shifts_y) if shifts_y else None,
    )


class JsonReadRunsBulkInput(BaseModel):
    beamtime_id: int
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


def _encode_experiment_type(a: DBExperimentType) -> JsonExperimentType:
    return JsonExperimentType(
        id=a.id,
        name=a.name,
        attributi=[
            JsonAttributiIdAndRole(id=ea.attributo_id, role=ea.chemical_role)
            for ea in a.attributi
        ],
    )


class JsonReadRunsBulkOutput(BaseModel):
    chemicals: list[JsonChemical]
    attributi: list[JsonAttributo]
    attributi_values: list[JsonAttributoBulkValue]
    experiment_types: list[JsonExperimentType]
    experiment_type_ids: list[int]


@app.post("/api/runs-bulk", tags=["runs"], response_model_exclude_defaults=True)
async def read_runs_bulk(
    input_: JsonReadRunsBulkInput, db: AsyncDB = Depends(get_db)
) -> JsonReadRunsBulkOutput:
    async with db.read_only_connection() as conn:
        beamtime_id = BeamtimeId(input_.beamtime_id)
        attributi = await db.retrieve_attributi(
            conn, beamtime_id, associated_table=None
        )
        chemicals = await db.retrieve_chemicals(conn, beamtime_id, attributi)
        all_runs = [
            r
            for r in await db.retrieve_runs(conn, beamtime_id, attributi)
            if r.external_id in input_.external_run_ids
        ]
        to_internal_id: dict[RunExternalId, RunInternalId] = {
            r.external_id: r.id for r in all_runs
        }
        internal_run_ids: list[RunInternalId] = []
        for external_run_id in input_.external_run_ids:
            internal_run_id = to_internal_id.get(RunExternalId(external_run_id))
            if internal_run_id is None:
                # There might be gaps in a run ID range that the user provided. Just ignore that.
                continue
            internal_run_ids.append(internal_run_id)
        if not internal_run_ids:
            raise HTTPException(
                status_code=400,
                detail="didn't receive any (valid) run IDs",
            )
        return JsonReadRunsBulkOutput(
            chemicals=[_encode_chemical(s) for s in chemicals],
            attributi=[
                _encode_attributo(a)
                for a in attributi
                if a.associated_table == AssociatedTable.RUN
            ],
            attributi_values=[
                JsonAttributoBulkValue(
                    attributo_id=attributo_id,
                    values=[_encode_attributo_value(attributo_id, v) for v in values],
                )
                for attributo_id, values in (
                    await db.retrieve_bulk_run_attributi(
                        conn,
                        beamtime_id,
                        attributi,
                        internal_run_ids,
                    )
                ).items()
            ],
            experiment_types=[
                _encode_experiment_type(a)
                for a in await db.retrieve_experiment_types(
                    conn, BeamtimeId(input_.beamtime_id)
                )
            ],
            experiment_type_ids=list(set(r.experiment_type_id for r in all_runs)),
        )


class JsonUpdateRunsBulkInput(BaseModel):
    beamtime_id: int
    external_run_ids: list[int]
    attributi: list[JsonAttributoValue]
    new_experiment_type_id: None | int


class JsonUpdateRunsBulkOutput(BaseModel):
    result: bool


@app.patch("/api/runs-bulk", tags=["runs"], response_model_exclude_defaults=True)
async def update_runs_bulk(
    input_: JsonUpdateRunsBulkInput, db: AsyncDB = Depends(get_db)
) -> JsonUpdateRunsBulkOutput:
    async with db.begin() as conn:
        beamtime_id = BeamtimeId(input_.beamtime_id)
        attributi = await db.retrieve_attributi(
            conn, beamtime_id, associated_table=None
        )
        to_internal_id: dict[RunExternalId, RunInternalId] = {
            r.external_id: r.id
            for r in await db.retrieve_runs(conn, beamtime_id, attributi)
        }
        internal_run_ids: set[RunInternalId] = set()

        for external_run_id in input_.external_run_ids:
            internal_run_id = to_internal_id.get(RunExternalId(external_run_id))
            if internal_run_id is None:
                # There might be gaps in a run ID range that the user provided. Just ignore that.
                continue
            internal_run_ids.add(internal_run_id)
        if not internal_run_ids:
            raise HTTPException(
                status_code=400,
                detail="didn't receive any (valid) run IDs",
            )
        await db.update_bulk_run_attributi(
            conn=conn,
            new_experiment_type_id=input_.new_experiment_type_id,
            attributi=attributi,
            beamtime_id=beamtime_id,
            run_ids=internal_run_ids,
            attributi_values=_json_attributo_list_to_attributi_map(
                attributi, input_.attributi
            ),
        )
        return JsonUpdateRunsBulkOutput(result=True)


def extract_runs_and_event_dates(
    runs: list[DBRunOutput], events: list[DBEvent]
) -> list[str]:
    set_of_dates: set[str] = set()
    for run in runs:
        started_date = run.started
        set_of_dates.add(started_date.strftime(DATE_FORMAT))
    for event in events:
        set_of_dates.add(event.created.strftime(DATE_FORMAT))

    return sorted(set_of_dates, reverse=True)


def event_has_date(event: DBEvent, date_filter: str) -> bool:
    return event.created.strftime(DATE_FORMAT) == date_filter


class JsonAnalysisRun(BaseModel):
    id: int
    attributi: list[JsonAttributoValue]


class JsonIndexingFom(BaseModel):
    hit_rate: float
    indexing_rate: float
    indexed_frames: int
    detector_shift_x_mm: None | float
    detector_shift_y_mm: None | float


def _encode_summary(summary: DBIndexingFOM) -> JsonIndexingFom:
    return JsonIndexingFom(
        hit_rate=summary.hit_rate,
        indexing_rate=summary.indexing_rate,
        indexed_frames=summary.indexed_frames,
        detector_shift_x_mm=summary.detector_shift_x_mm,
        detector_shift_y_mm=summary.detector_shift_y_mm,
    )


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


class JsonReadRunAnalysis(BaseModel):
    chemicals: list[JsonChemical]
    attributi: list[JsonAttributo]
    runs: list[JsonAnalysisRun]
    indexing_results_by_run_id: list[JsonRunAnalysisIndexingResult]


@app.get(
    "/api/run-analysis/{beamtimeId}",
    tags=["analysis"],
    response_model_exclude_defaults=True,
)
async def read_run_analysis(
    beamtimeId: BeamtimeId, db: AsyncDB = Depends(get_db)
) -> JsonReadRunAnalysis:
    async with db.read_only_connection() as conn:

        def extract_summary(o: DBIndexingResultOutput) -> DBIndexingFOM:
            if isinstance(
                o.runtime_status, (DBIndexingResultRunning, DBIndexingResultDone)
            ):
                return o.runtime_status.fom
            return empty_indexing_fom

        all_indexing_results = await db.retrieve_indexing_results(
            conn, beamtime_id=beamtimeId
        )
        ir_to_stats: dict[int, list[DBIndexingResultStatistic]] = group_by(
            await db.retrieve_indexing_result_statistics(
                conn, beamtimeId, indexing_result_id=None
            ),
            lambda irs: irs.indexing_result_id,
        )
        attributi = await db.retrieve_attributi(conn, beamtimeId, associated_table=None)
        chemicals = await db.retrieve_chemicals(conn, beamtimeId, attributi)
        runs = await db.retrieve_runs(conn, beamtimeId, attributi)
        run_internal_to_external_id = {r.id: r.external_id for r in runs}
        return JsonReadRunAnalysis(
            chemicals=[_encode_chemical(s) for s in chemicals],
            attributi=[_encode_attributo(a) for a in attributi],
            runs=[
                JsonAnalysisRun(
                    id=r.external_id, attributi=_encode_attributi_map(r.attributi)
                )
                for r in runs
            ],
            indexing_results_by_run_id=[
                JsonRunAnalysisIndexingResult(
                    run_id=run_internal_to_external_id[rid],
                    # foms = figures of merit
                    foms=[
                        _encode_summary(extract_summary(indexing_result))
                        for indexing_result in indexing_results_for_run
                    ],
                    indexing_statistics=[
                        JsonIndexingStatistic(
                            time=datetime_to_attributo_int(stat.time),
                            frames=stat.frames,
                            hits=stat.hits,
                            indexed=stat.indexed_frames,
                            crystals=stat.indexed_crystals,
                        )
                        # slightly confusing but super handy
                        # double-list comprehension: first, get all
                        # indexing result for the run...
                        for indexing_result_for_run in indexing_results_for_run
                        # ...then, get all the statistics points
                        # inside the indexing result, and concatenate
                        # this list of lists
                        for stat in ir_to_stats.get(indexing_result_for_run.id, [])
                    ],
                )
                for rid, indexing_results_for_run in group_by(
                    all_indexing_results, lambda ir: ir.run_id
                ).items()
            ],
        )


def _encode_event(e: DBEvent) -> JsonEvent:
    return JsonEvent(
        id=e.id,
        text=e.text,
        source=e.source,
        created=datetime_to_attributo_int(e.created),
        level=e.level.value,
        files=[_encode_file_output(f) for f in e.files],
    )


class JsonDataSet(BaseModel):
    id: int
    experiment_type_id: int
    attributi: list[JsonAttributoValue]
    summary: None | JsonIndexingFom


class JsonRun(BaseModel):
    id: int
    external_id: int
    attributi: list[JsonAttributoValue]
    started: int
    stopped: None | int
    files: list[JsonFileOutput]
    summary: JsonIndexingFom
    experiment_type_id: int
    data_sets: list[int]
    running_indexing_jobs: list[int]


class JsonReadRuns(BaseModel):
    live_stream_file_id: None | int
    filter_dates: list[str]
    runs: list[JsonRun]
    attributi: list[JsonAttributo]
    experiment_types: list[JsonExperimentType]
    data_sets: list[JsonDataSet]
    events: list[JsonEvent]
    chemicals: list[JsonChemical]
    user_config: JsonUserConfig


def _indexing_fom_for_run(
    indexing_results_for_runs: dict[int, list[DBIndexingResultOutput]], run: DBRunOutput
) -> DBIndexingFOM:
    indexing_results_for_run = sorted(
        [
            (ir.id, ir.runtime_status)
            for ir in indexing_results_for_runs.get(run.id, [])
            if isinstance(
                ir.runtime_status,
                (DBIndexingResultDone, DBIndexingResultRunning),
            )
        ],
        key=lambda ir: ir[0],
        reverse=True,
    )
    if indexing_results_for_run:
        return indexing_results_for_run[0][1].fom
    return empty_indexing_fom


def _encode_data_set(a: DBDataSet, summary: DBIndexingFOM | None) -> JsonDataSet:
    return JsonDataSet(
        id=a.id,
        experiment_type_id=a.experiment_type_id,
        attributi=_encode_attributi_map(a.attributi),
        summary=_encode_summary(summary) if summary is not None else None,
    )


@app.get("/api/runs/{beamtimeId}", tags=["runs"], response_model_exclude_defaults=True)
async def read_runs(
    beamtimeId: BeamtimeId,
    date: None | str = None,
    # pylint: disable=redefined-builtin
    filter: None | str = None,
    db: AsyncDB = Depends(get_db),
) -> JsonReadRuns:
    async with db.read_only_connection() as conn:
        attributi = await db.retrieve_attributi(conn, beamtimeId, associated_table=None)
        attributi.sort(key=attributo_sort_key)
        chemicals = await db.retrieve_chemicals(conn, beamtimeId, attributi)
        experiment_types = await db.retrieve_experiment_types(conn, beamtimeId)
        data_sets = await db.retrieve_data_sets(conn, beamtimeId, attributi)
        all_runs = await db.retrieve_runs(conn, beamtimeId, attributi)
        all_events = await db.retrieve_events(conn, beamtimeId, EventLogLevel.USER)

        try:
            run_filter = compile_run_filter(
                filter.strip() if filter is not None else ""
            )
            runs = [
                run
                for run in all_runs
                if run_filter(
                    FilterInput(
                        run=run,
                        chemical_names={s.name: s.id for s in chemicals},
                        attributo_name_to_id={
                            a.name: a.id
                            for a in attributi
                            if a.associated_table == AssociatedTable.RUN
                        },
                    )
                )
            ]
        except FilterParseError as e:
            raise Exception(f"error in filter string: {e}")

        date_filter = date.strip() if date is not None else ""
        runs = (
            [run for run in runs if run_has_started_date(run, date_filter)]
            if date_filter
            else runs
        )
        events = (
            [event for event in all_events if event_has_date(event, date_filter)]
            if date_filter
            else all_events
        )

        indexing_results = await db.retrieve_indexing_results(
            conn, beamtime_id=beamtimeId
        )
        indexing_results_for_runs: dict[int, list[DBIndexingResultOutput]] = group_by(
            indexing_results, lambda ir: ir.run_id
        )
        run_foms: dict[int, DBIndexingFOM] = {
            r.id: _indexing_fom_for_run(indexing_results_for_runs, r) for r in runs
        }
        data_set_id_to_grouped: dict[int, DBIndexingFOM] = {
            ds.id: _summary_from_foms(
                [
                    run_foms.get(r.id, empty_indexing_fom)
                    for r in runs
                    if r.experiment_type_id == ds.experiment_type_id
                    and run_matches_dataset(r.attributi, ds.attributi)
                ]
            )
            for ds in data_sets
        }

        user_configuration = await db.retrieve_configuration(conn, beamtimeId)
        return JsonReadRuns(
            live_stream_file_id=await db.retrieve_file_id_by_name(
                conn, live_stream_image_name(beamtimeId)
            ),
            filter_dates=extract_runs_and_event_dates(all_runs, all_events),
            attributi=[_encode_attributo(a) for a in attributi],
            events=[_encode_event(e) for e in events],
            chemicals=[_encode_chemical(a) for a in chemicals],
            user_config=_encode_user_configuration(user_configuration),
            experiment_types=[_encode_experiment_type(a) for a in experiment_types],
            data_sets=[
                _encode_data_set(a, data_set_id_to_grouped.get(a.id, None))
                for a in data_sets
            ],
            runs=[
                JsonRun(
                    id=r.id,
                    external_id=r.external_id,
                    attributi=_encode_attributi_map(r.attributi),
                    started=datetime_to_attributo_int(r.started),
                    stopped=(
                        datetime_to_attributo_int(r.stopped)
                        if r.stopped is not None
                        else None
                    ),
                    files=[_encode_file_output(f) for f in r.files],
                    summary=_encode_summary(run_foms.get(r.id, empty_indexing_fom)),
                    experiment_type_id=r.experiment_type_id,
                    data_sets=[
                        ds.id
                        for ds in data_sets
                        if r.experiment_type_id == ds.experiment_type_id
                        and run_matches_dataset(r.attributi, ds.attributi)
                    ],
                    running_indexing_jobs=[
                        ir.id
                        for ir in indexing_results
                        if ir.run_id == r.id
                        and isinstance(ir.runtime_status, DBIndexingResultRunning)
                    ],
                )
                for r in runs
            ],
        )


class JsonCreateFileOutput(BaseModel):
    id: int
    file_name: str
    description: str
    type_: str
    size_in_bytes: int
    original_path: None | str


@app.post("/api/files", tags=["files"], response_model_exclude_defaults=True)
async def create_file(
    file: UploadFile, description: Annotated[str, Form()], db: AsyncDB = Depends(get_db)
) -> JsonCreateFileOutput:
    async with db.begin() as conn:
        file_name = file.filename if file.filename is not None else "nofilename"

        # Since we potentially need to seek around in the file, and we don't know if it's a seekable
        # stream (I think?) we store it in a named temp file first.
        with NamedTemporaryFile(mode="w+b") as temp_file:
            temp_file.write(file.file.read())
            temp_file.flush()
            temp_file.seek(0, os.SEEK_SET)

            create_result = await db.create_file(
                conn,
                file_name=file_name,
                description=description,
                original_path=None,
                contents_location=Path(temp_file.name),
                deduplicate=False,
            )

    return JsonCreateFileOutput(
        id=create_result.id,
        file_name=file_name,
        description=description,
        type_=create_result.type_,
        size_in_bytes=create_result.size_in_bytes,
        original_path=None,
    )


@app.post(
    "/api/files/simple/{extension}",
    tags=["files"],
    description="""This endpoint was specifically crafted to upload files in a simple way from external scripts, such as the CrystFEL scripts.
    It doesn't need a multipart request and the file extension can be set using the path parameter (which is used to generate nice
    .mtz and .pdb download URLs).
    """,
    response_model_exclude_defaults=True,
)
async def create_file_simple(
    extension: str, request: Request, db: AsyncDB = Depends(get_db)
) -> JsonCreateFileOutput:
    async with db.begin() as conn:
        # Since we potentially need to seek around in the file, and we don't know if it's a seekable
        # stream (I think?) we store it in a named temp file first.
        with NamedTemporaryFile(mode="w+b") as temp_file:
            temp_file.write(await request.body())
            temp_file.flush()
            temp_file.seek(0, os.SEEK_SET)

            file_name = f"{uuid.uuid4()}.{extension}"
            create_result = await db.create_file(
                conn,
                file_name=file_name,
                description="",
                original_path=None,
                contents_location=Path(temp_file.name),
                deduplicate=False,
            )

    return JsonCreateFileOutput(
        id=create_result.id,
        file_name=file_name,
        description="",
        type_=create_result.type_,
        size_in_bytes=create_result.size_in_bytes,
        # Doesn't really make sense here
        original_path=None,
    )


class JsonUserConfigurationSingleOutput(BaseModel):
    value_bool: None | bool
    value_int: None | int


@app.get(
    "/api/user-config/{beamtimeId}/{key}",
    tags=["config"],
    response_model_exclude_defaults=True,
)
async def read_user_configuration_single(
    beamtimeId: BeamtimeId, key: str, db: AsyncDB = Depends(get_db)
) -> JsonUserConfigurationSingleOutput:
    async with db.read_only_connection() as conn:
        user_configuration = await db.retrieve_configuration(conn, beamtimeId)
        if key == USER_CONFIGURATION_AUTO_PILOT:
            return JsonUserConfigurationSingleOutput(
                value_bool=user_configuration.auto_pilot, value_int=None
            )
        if key == USER_CONFIGURATION_ONLINE_CRYSTFEL:
            return JsonUserConfigurationSingleOutput(
                value_bool=user_configuration.use_online_crystfel, value_int=None
            )
        if key == USER_CONFIGURATION_CURRENT_EXPERIMENT_TYPE_ID:
            return JsonUserConfigurationSingleOutput(
                value_int=user_configuration.current_experiment_type_id, value_bool=None
            )
        raise Exception(
            f"Couldn't find config key {key}, only know "
            + ", ".join(f'"{x}"' for x in KNOWN_USER_CONFIGURATION_VALUES)
        )


@app.patch(
    "/api/user-config/{beamtimeId}/{key}/{value}",
    tags=["config"],
    response_model_exclude_defaults=True,
)
async def update_user_configuration_single(
    beamtimeId: BeamtimeId, key: str, value: str, db: AsyncDB = Depends(get_db)
) -> JsonUserConfigurationSingleOutput:
    async with db.begin() as conn:
        user_configuration = await db.retrieve_configuration(conn, beamtimeId)
        if key == USER_CONFIGURATION_AUTO_PILOT:
            new_value = value == "True"

            new_configuration = replace(
                user_configuration,
                auto_pilot=new_value,
            )
            await db.update_configuration(
                conn,
                beamtimeId,
                new_configuration,
            )
            return JsonUserConfigurationSingleOutput(
                value_bool=new_value, value_int=None
            )
        if key == USER_CONFIGURATION_ONLINE_CRYSTFEL:
            new_value = value == "True"

            new_configuration = replace(
                user_configuration,
                use_online_crystfel=new_value,
            )
            await db.update_configuration(
                conn,
                beamtimeId,
                new_configuration,
            )
            return JsonUserConfigurationSingleOutput(
                value_bool=new_value, value_int=None
            )
        if key == USER_CONFIGURATION_CURRENT_EXPERIMENT_TYPE_ID:
            new_configuration = replace(
                user_configuration,
                current_experiment_type_id=int(value),
            )
            await db.update_configuration(
                conn,
                beamtimeId,
                new_configuration,
            )
            return JsonUserConfigurationSingleOutput(
                value_int=int(value), value_bool=None
            )
        raise Exception(
            f"Couldn't find config key {key}, only know "
            + ", ".join(f'"{x}"' for x in KNOWN_USER_CONFIGURATION_VALUES)
        )


class JsonCreateExperimentTypeInput(BaseModel):
    name: str
    beamtime_id: int
    attributi: list[JsonAttributiIdAndRole]


class JsonCreateExperimentTypeOutput(BaseModel):
    id: int


@app.post(
    "/api/experiment-types",
    tags=["experimenttypes"],
    response_model_exclude_defaults=True,
)
async def create_experiment_type(
    input_: JsonCreateExperimentTypeInput, db: AsyncDB = Depends(get_db)
) -> JsonCreateExperimentTypeOutput:
    async with db.begin() as conn:
        et_id = await db.create_experiment_type(
            conn,
            name=input_.name,
            beamtime_id=BeamtimeId(input_.beamtime_id),
            experiment_attributi=[
                AttributoIdAndRole(attributo_id=AttributoId(a.id), chemical_role=a.role)
                for a in input_.attributi
            ],
        )

    return JsonCreateExperimentTypeOutput(id=et_id)


class JsonChangeRunExperimentType(BaseModel):
    run_internal_id: int
    beamtime_id: int
    experiment_type_id: None | int


class JsonChangeRunExperimentTypeOutput(BaseModel):
    result: bool


@app.post(
    "/api/experiment-types/change-for-run",
    tags=["experimenttypes"],
    response_model_exclude_defaults=True,
)
async def change_current_run_experiment_type(
    input_: JsonChangeRunExperimentType, db: AsyncDB = Depends(get_db)
) -> JsonChangeRunExperimentTypeOutput:
    async with db.begin() as conn:
        run_id = RunInternalId(input_.run_internal_id)
        beamtime_id = BeamtimeId(input_.beamtime_id)
        attributi = await db.retrieve_attributi(conn, beamtime_id, None)
        run = await db.retrieve_run(
            conn,
            run_id,
            attributi,
        )
        new_experiment_type = input_.experiment_type_id
        # There is no semantic yet for resetting an experiment type
        if new_experiment_type is None:
            return JsonChangeRunExperimentTypeOutput(result=False)

        experiment_type_instance = next(
            iter(
                x
                for x in await db.retrieve_experiment_types(conn, beamtime_id)
                if x.id == new_experiment_type
            ),
            None,
        )
        if experiment_type_instance is None:
            raise Exception(
                f"Couldn't find experiment type with ID {new_experiment_type}"
            )
        experiment_type_attributo_names = {
            a.attributo_id for a in experiment_type_instance.attributi
        }
        for a in attributi:
            if (
                a.group == ATTRIBUTO_GROUP_MANUAL
                and a.id not in experiment_type_attributo_names
            ):
                run.attributi.remove_but_keep_type(a.id)
        await db.update_run(
            conn,
            internal_id=run_id,
            stopped=run.stopped,
            attributi=run.attributi,
            new_experiment_type_id=None,
        )
        await db.update_configuration(
            conn,
            beamtime_id,
            replace(
                await db.retrieve_configuration(conn, beamtime_id),
                current_experiment_type_id=new_experiment_type,
            ),
        )
        return JsonChangeRunExperimentTypeOutput(result=True)


class JsonExperimentTypeAndRuns(BaseModel):
    id: int
    runs: list[str]


class JsonReadExperimentTypes(BaseModel):
    experiment_types: list[JsonExperimentType]
    attributi: list[JsonAttributo]
    experiment_type_id_to_run: list[JsonExperimentTypeAndRuns]


@app.get(
    "/api/experiment-types/{beamtimeId}",
    tags=["experimenttypes"],
    response_model_exclude_defaults=True,
)
async def read_experiment_types(
    beamtimeId: BeamtimeId, db: AsyncDB = Depends(get_db)
) -> JsonReadExperimentTypes:
    async with db.read_only_connection() as conn:
        attributi = await db.retrieve_attributi(
            conn, beamtimeId, associated_table=AssociatedTable.RUN
        )
        runs = await db.retrieve_runs(conn, beamtimeId, attributi)
        experiment_type_id_to_runs: dict[int, list[DBRunOutput]] = group_by(
            runs, lambda r: r.experiment_type_id
        )
        return JsonReadExperimentTypes(
            experiment_types=[
                _encode_experiment_type(a)
                for a in await db.retrieve_experiment_types(conn, beamtimeId)
            ],
            attributi=[_encode_attributo(a) for a in attributi],
            experiment_type_id_to_run=[
                JsonExperimentTypeAndRuns(
                    id=et_id,
                    runs=format_run_id_intervals(r.external_id for r in runs),
                )
                for et_id, runs in experiment_type_id_to_runs.items()
            ],
        )


class JsonDeleteExperimentType(BaseModel):
    id: int


class JsonDeleteExperimentTypeOutput(BaseModel):
    result: bool


@app.delete("/api/experiment-types", tags=["experimenttypes"])
async def delete_experiment_type(
    input_: JsonDeleteExperimentType, db: AsyncDB = Depends(get_db)
) -> JsonDeleteExperimentTypeOutput:
    async with db.begin() as conn:
        await db.delete_experiment_type(conn, input_.id)

    return JsonDeleteExperimentTypeOutput(result=True)


class JsonBeamtimeScheduleRow(BaseModel):
    users: str
    date: str
    shift: str
    comment: str
    td_support: str
    chemicals: list[int]


class JsonBeamtimeSchedule(BaseModel):
    schedule: list[JsonBeamtimeScheduleRow]


@app.get(
    "/api/schedule/{beamtimeId}",
    tags=["schedule"],
    response_model_exclude_defaults=True,
)
async def get_beamtime_schedule(
    beamtimeId: BeamtimeId, db: AsyncDB = Depends(get_db)
) -> JsonBeamtimeSchedule:
    async with db.read_only_connection() as conn:
        schedule = await db.retrieve_beamtime_schedule(conn, beamtimeId)
        return JsonBeamtimeSchedule(
            schedule=[
                JsonBeamtimeScheduleRow(
                    users=shift_dict.users,
                    date=shift_dict.date,
                    shift=shift_dict.shift,
                    comment=shift_dict.comment,
                    td_support=shift_dict.td_support,
                    chemicals=shift_dict.chemicals,
                )
                for shift_dict in schedule
            ]
        )


class JsonUpdateBeamtimeScheduleInput(BaseModel):
    beamtime_id: int
    schedule: list[JsonBeamtimeScheduleRow]


class JsonBeamtimeScheduleOutput(BaseModel):
    schedule: list[JsonBeamtimeScheduleRow]


@app.post("/api/schedule", tags=["schedule"], response_model_exclude_defaults=True)
async def update_beamtime_schedule(
    input_: JsonUpdateBeamtimeScheduleInput, db: AsyncDB = Depends(get_db)
) -> JsonBeamtimeScheduleOutput:
    beamtime_id = BeamtimeId(input_.beamtime_id)
    async with db.begin() as conn:
        await db.replace_beamtime_schedule(
            conn=conn,
            beamtime_id=beamtime_id,
            schedule=[
                BeamtimeScheduleEntry(
                    users=shift_dict.users,
                    date=shift_dict.date,
                    shift=shift_dict.shift,
                    comment=shift_dict.comment,
                    td_support=shift_dict.td_support,
                    chemicals=shift_dict.chemicals,
                )
                for shift_dict in input_.schedule
            ],
        )

    async with db.begin() as conn:
        schedule = await db.retrieve_beamtime_schedule(conn, beamtime_id)
        return JsonBeamtimeScheduleOutput(
            schedule=[
                JsonBeamtimeScheduleRow(
                    users=shift_dict.users,
                    date=shift_dict.date,
                    shift=shift_dict.shift,
                    comment=shift_dict.comment,
                    td_support=shift_dict.td_support,
                    chemicals=shift_dict.chemicals,
                )
                for shift_dict in schedule
            ]
        )


class JsonCreateLiveStreamSnapshotOutput(BaseModel):
    id: int
    file_name: str
    description: str
    type_: str
    size_in_bytes: int
    original_path: None | str


@app.get(
    "/api/live-stream/snapshot/{beamtimeId}",
    tags=["events"],
    response_model_exclude_defaults=True,
)
async def create_live_stream_snapshot(
    beamtimeId: int, db: AsyncDB = Depends(get_db)
) -> JsonCreateLiveStreamSnapshotOutput:
    async with db.begin() as conn:
        existing_live_stream = await db.retrieve_file_id_by_name(
            conn, live_stream_image_name(beamtimeId)
        )
        if existing_live_stream is None:
            raise Exception("Cannot make a snapshot of a non-existing image!")
        new_file_name = live_stream_image_name(beamtimeId) + "-copy"
        new_image = await db.duplicate_file(conn, existing_live_stream, new_file_name)
        return JsonCreateLiveStreamSnapshotOutput(
            id=new_image.id,
            file_name=new_file_name,
            description="",
            type_=new_image.type_,
            size_in_bytes=new_image.size_in_bytes,
            # Doesn't really make sense here
            original_path=None,
        )


class JsonUpdateLiveStream(BaseModel):
    id: int


@app.post("/api/live-stream/{beamtimeId}", response_model_exclude_defaults=True)
async def update_live_stream(
    file: UploadFile, beamtimeId: int, db: AsyncDB = Depends(get_db)
) -> JsonUpdateLiveStream:
    async with db.begin() as conn:
        # Since we potentially need to seek around in the file, and we don't know if it's a seekable
        # stream (I think?) we store it in a named temp file first.
        with NamedTemporaryFile(mode="w+b") as temp_file:
            temp_file.write(file.file.read())
            temp_file.flush()
            temp_file.seek(0, os.SEEK_SET)

            image_file_name = live_stream_image_name(beamtimeId)
            existing_live_stream = await db.retrieve_file_id_by_name(
                conn, image_file_name
            )

            file_id: int
            if existing_live_stream is not None:
                file_id = existing_live_stream
                await db.update_file(
                    conn, existing_live_stream, contents_location=Path(temp_file.name)
                )
            else:
                file_id = (
                    await db.create_file(
                        conn,
                        file_name=image_file_name,
                        description="Live stream image",
                        original_path=None,
                        contents_location=Path(temp_file.name),
                        deduplicate=False,
                    )
                ).id

            return JsonUpdateLiveStream(id=file_id)


class JsonCreateDataSetFromRun(BaseModel):
    run_internal_id: int
    experiment_type_id: int
    beamtime_id: int


class JsonCreateDataSetFromRunOutput(BaseModel):
    data_set_id: int


@app.post(
    "/api/data-sets/from-run", tags=["datasets"], response_model_exclude_defaults=True
)
async def create_data_set_from_run(
    input_: JsonCreateDataSetFromRun, db: AsyncDB = Depends(get_db)
) -> JsonCreateDataSetFromRunOutput:
    async with db.begin() as conn:
        run_id = RunInternalId(input_.run_internal_id)
        experiment_type_id = input_.experiment_type_id
        beamtime_id = BeamtimeId(input_.beamtime_id)

        experiment_type_resolved: None | DBExperimentType = next(
            iter(
                et
                for et in await db.retrieve_experiment_types(conn, beamtime_id)
                if et.id == experiment_type_id
            ),
            None,
        )

        if experiment_type_resolved is None:
            raise Exception(
                f"Couldn't find experiment type with ID {experiment_type_id}"
            )

        attributi = await db.retrieve_attributi(conn, beamtime_id, AssociatedTable.RUN)

        run = await db.retrieve_run(conn, run_id, attributi)

        experiment_type_attributo_names: set[AttributoId] = {
            a.attributo_id for a in experiment_type_resolved.attributi
        }
        attributi_map = AttributiMap(
            {a.id: a for a in attributi},
            {an: run.attributi.select(an) for an in experiment_type_attributo_names},
        )

        id_ = await db.create_data_set(
            conn, beamtime_id, experiment_type_id, attributi_map
        )

        return JsonCreateDataSetFromRunOutput(data_set_id=id_)


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


@app.post("/api/refinement-results", tags=["refinements"])
async def create_refinement_result(
    input_: JsonCreateRefinementResultInput, db: AsyncDB = Depends(get_db)
) -> JsonCreateRefinementResultOutput:
    async with db.begin() as conn:
        refinement_result_id = await db.create_refinement_result(
            conn,
            merge_result_id=input_.merge_result_id,
            pdb_file_id=input_.pdb_file_id,
            mtz_file_id=input_.mtz_file_id,
            rfree=input_.r_free,
            rwork=input_.r_work,
            rms_bond_angle=input_.rms_bond_angle,
            rms_bond_length=input_.rms_bond_length,
        )
        return JsonCreateRefinementResultOutput(id=refinement_result_id)


class JsonCreateDataSetInput(BaseModel):
    beamtime_id: int
    experiment_type_id: int
    attributi: list[JsonAttributoValue]


class JsonCreateDataSetOutput(BaseModel):
    id: int


@app.post("/api/data-sets", tags=["datasets"], response_model_exclude_defaults=True)
async def create_data_set(
    input_: JsonCreateDataSetInput, db: AsyncDB = Depends(get_db)
) -> JsonCreateDataSetOutput:
    async with db.begin() as conn:
        beamtime_id = BeamtimeId(input_.beamtime_id)
        attributi = await db.retrieve_attributi(
            conn, beamtime_id, associated_table=None
        )
        attributi_by_id: dict[int, DBAttributo] = {a.id: a for a in attributi}
        previous_data_sets = await db.retrieve_data_sets(conn, beamtime_id, attributi)

        experiment_type_id = input_.experiment_type_id
        experiment_type: None | DBExperimentType = next(
            iter(
                t
                for t in await db.retrieve_experiment_types(
                    conn,
                    beamtime_id,
                )
                if t.id == experiment_type_id
            ),
            None,
        )

        if experiment_type is None:
            raise Exception(f"Experiment type with ID {experiment_type_id} not found")

        data_set_attributi = input_.attributi
        input_attributi_as_map = _json_attributo_list_to_attributi_map(
            attributi, input_.attributi
        )
        if any(
            x
            for x in previous_data_sets
            if x.experiment_type_id == experiment_type_id
            and x.attributi.to_json() == input_attributi_as_map
        ):
            raise Exception("This data set already exists!")

        if not data_set_attributi:
            raise Exception("You have to set a least one attributo value")

        processed_attributi: dict[int, AttributoValue] = {}
        experiment_type_attributo_names: set[AttributoId] = {
            a.attributo_id for a in experiment_type.attributi
        }
        for attributo_id in experiment_type_attributo_names:
            in_input = input_attributi_as_map.select(attributo_id)
            if in_input is not None:
                processed_attributi[int(attributo_id)] = in_input
            else:
                attributo = attributi_by_id.get(attributo_id, None)
                assert (
                    attributo is not None
                ), f"attributo {attributo_id} mentioned in experiment type {experiment_type_id} not found"
                if not isinstance(attributo.attributo_type, AttributoTypeBoolean):
                    raise Exception(f'Got no value for attributo "{attributo_id}"')
                processed_attributi[int(attributo_id)] = False

        data_set_id = await db.create_data_set(
            conn,
            beamtime_id,
            experiment_type_id=experiment_type_id,
            attributi=AttributiMap.from_types_and_raw(
                attributi,
                processed_attributi,
            ),
        )

    return JsonCreateDataSetOutput(id=data_set_id)


class JsonReadDataSets(BaseModel):
    data_sets: list[JsonDataSet]
    chemicals: list[JsonChemical]
    attributi: list[JsonAttributo]
    experiment_types: list[JsonExperimentType]


@app.get(
    "/api/data-sets/{beamtimeId}",
    tags=["datasets"],
    response_model_exclude_defaults=True,
)
async def read_data_sets(
    beamtimeId: BeamtimeId, db: AsyncDB = Depends(get_db)
) -> JsonReadDataSets:
    async with db.read_only_connection() as conn:
        attributi = list(
            await db.retrieve_attributi(conn, beamtimeId, associated_table=None)
        )
        chemicals = await db.retrieve_chemicals(conn, beamtimeId, attributi)
        experiment_types = await db.retrieve_experiment_types(conn, beamtimeId)
        return JsonReadDataSets(
            data_sets=[
                _encode_data_set(a, summary=None)
                for a in await db.retrieve_data_sets(
                    conn,
                    beamtimeId,
                    attributi,
                )
            ],
            chemicals=[_encode_chemical(s) for s in chemicals],
            attributi=[_encode_attributo(a) for a in attributi],
            experiment_types=[_encode_experiment_type(a) for a in experiment_types],
        )


class JsonDeleteDataSetInput(BaseModel):
    id: int


class JsonDeleteDataSetOutput(BaseModel):
    result: bool


@app.delete("/api/data-sets", tags=["datasets"], response_model_exclude_defaults=True)
async def delete_data_set(
    input_: JsonDeleteDataSetInput, db: AsyncDB = Depends(get_db)
) -> JsonDeleteDataSetOutput:
    async with db.begin() as conn:
        await db.delete_data_set(conn, id_=input_.id)

    return JsonDeleteDataSetOutput(result=True)


class JsonDeleteEventInput(BaseModel):
    id: int


class JsonDeleteEventOutput(BaseModel):
    result: bool


@app.delete("/api/events", tags=["events"], response_model_exclude_defaults=True)
async def delete_event(
    input_: JsonDeleteEventInput, db: AsyncDB = Depends(get_db)
) -> JsonDeleteEventOutput:
    async with db.begin() as conn:
        await db.delete_event(conn, input_.id)

    return JsonDeleteEventOutput(result=True)


class JsonReadEvents(BaseModel):
    events: list[JsonEvent]


@app.get(
    "/api/events/{beamtimeId}", tags=["events"], response_model_exclude_defaults=True
)
async def read_events(
    beamtimeId: BeamtimeId,
    db: AsyncDB = Depends(get_db),
) -> JsonReadEvents:
    async with db.read_only_connection() as conn:
        return JsonReadEvents(
            events=[
                _encode_event(e)
                for e in await db.retrieve_events(conn, beamtimeId, None)
            ]
        )


class JsonDeleteFileInput(BaseModel):
    id: int


class JsonDeleteFileOutput(BaseModel):
    id: int


@app.delete("/api/files", tags=["files"], response_model_exclude_defaults=True)
async def delete_file(
    input_: JsonDeleteFileInput, db: AsyncDB = Depends(get_db)
) -> JsonDeleteFileOutput:
    async with db.begin() as conn:
        await db.delete_file(conn, input_.id)

    return JsonDeleteFileOutput(id=input_.id)


class JsonCreateAttributiFromSchemaSingleAttributo(BaseModel):
    attributo_name: str
    attributo_type: JSONSchemaUnion = Field(discriminator="type")
    description: None | str = None


class JsonCreateAttributiFromSchemaInput(BaseModel):
    attributi_schema: list[JsonCreateAttributiFromSchemaSingleAttributo]
    beamtime_id: int


class JsonCreateAttributiFromSchemaOutput(BaseModel):
    created_attributi: int


@app.post(
    "/api/attributi/schema", tags=["attributi"], response_model_exclude_defaults=True
)
async def create_attributi_from_schema(
    input_: JsonCreateAttributiFromSchemaInput, db: AsyncDB = Depends(get_db)
) -> JsonCreateAttributiFromSchemaOutput:
    logger.info("ingesting attributi schema")

    async with db.begin() as conn:
        attributi_schema = input_.attributi_schema
        beamtime_id = BeamtimeId(input_.beamtime_id)
        await ingest_run_attributi_schema(
            db,
            conn,
            beamtime_id,
            await db.retrieve_attributi(conn, beamtime_id, AssociatedTable.RUN),
            {
                s.attributo_name: (s.description, s.attributo_type)
                for s in attributi_schema
            },
            group=AUTOMATIC_ATTRIBUTI_GROUP,
        )
        return JsonCreateAttributiFromSchemaOutput(
            created_attributi=len(input_.attributi_schema)
        )


class JsonCreateAttributoInput(BaseModel):
    beamtime_id: int
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


@app.post("/api/attributi", tags=["attributi"], response_model_exclude_defaults=True)
async def create_attributo(
    input_: JsonCreateAttributoInput, db: AsyncDB = Depends(get_db)
) -> JsonCreateAttributoOutput:
    async with db.begin() as conn:
        attributo_id = await db.create_attributo(
            conn,
            beamtime_id=BeamtimeId(input_.beamtime_id),
            name=input_.name,
            description=input_.description,
            group=input_.group,
            associated_table=input_.associated_table,
            type_=schema_to_attributo_type(
                schema_number=input_.attributo_type_number,
                schema_boolean=input_.attributo_type_boolean,
                schema_integer=input_.attributo_type_integer,
                schema_array=input_.attributo_type_array,
                schema_string=input_.attributo_type_string,
            ),
        )

    return JsonCreateAttributoOutput(id=attributo_id)


class JsonUpdateAttributoConversionFlags(BaseModel):
    ignoreUnits: bool


class JsonUpdateAttributoInput(BaseModel):
    beamtime_id: int
    attributo: JsonAttributo
    conversion_flags: JsonUpdateAttributoConversionFlags


class JsonUpdateAttributoOutput(BaseModel):
    id: int


@app.patch("/api/attributi", tags=["attributi"], response_model_exclude_defaults=True)
async def update_attributo(
    input_: JsonUpdateAttributoInput, db: AsyncDB = Depends(get_db)
) -> JsonUpdateAttributoOutput:
    async with db.begin() as conn:
        id_ = input_.attributo.id
        beamtime_id = input_.beamtime_id
        new_attributo = DBAttributo(
            id=AttributoId(id_),
            beamtime_id=BeamtimeId(beamtime_id),
            name=input_.attributo.name,
            description=input_.attributo.description,
            group=input_.attributo.group,
            associated_table=input_.attributo.associated_table,
            attributo_type=schema_to_attributo_type(
                schema_number=input_.attributo.attributo_type_number,
                schema_boolean=input_.attributo.attributo_type_boolean,
                schema_integer=input_.attributo.attributo_type_integer,
                schema_array=input_.attributo.attributo_type_array,
                schema_string=input_.attributo.attributo_type_string,
            ),
        )
        await db.update_attributo(
            conn,
            id_=AttributoId(id_),
            beamtime_id=BeamtimeId(beamtime_id),
            conversion_flags=AttributoConversionFlags(
                ignore_units=input_.conversion_flags.ignoreUnits
            ),
            new_attributo=new_attributo,
        )

    return JsonUpdateAttributoOutput(id=input_.attributo.id)


class JsonCheckStandardUnitInput(BaseModel):
    input: str


class JsonCheckStandardUnitOutput(BaseModel):
    input: str
    error: None | str
    normalized: None | str


@app.post("/api/unit", response_model_exclude_defaults=True)
async def check_standard_unit(
    input_: JsonCheckStandardUnitInput,
) -> JsonCheckStandardUnitOutput:
    unit_input = input_.input

    if unit_input == "":
        return JsonCheckStandardUnitOutput(
            input=unit_input, error="Unit empty", normalized=None
        )

    try:
        return JsonCheckStandardUnitOutput(
            input=unit_input, error=None, normalized=f"{_UNIT_REGISTRY(unit_input):P}"
        )
    except:
        return JsonCheckStandardUnitOutput(
            input=unit_input, error="Invalid unit", normalized=None
        )


class JsonDeleteAttributoInput(BaseModel):
    id: int


class JsonDeleteAttributoOutput(BaseModel):
    id: int


@app.delete("/api/attributi", tags=["attributi"], response_model_exclude_defaults=True)
async def delete_attributo(
    input_: JsonDeleteAttributoInput, db: AsyncDB = Depends(get_db)
) -> JsonDeleteAttributoOutput:
    async with db.begin() as conn:
        attributo_id = AttributoId(input_.id)

        await db.delete_attributo(
            conn,
            attributo_id=attributo_id,
        )
        return JsonDeleteAttributoOutput(id=input_.id)


def _do_content_disposition(mime_type: str, extension: str) -> bool:
    if mime_type == "text/plain":
        return extension in ("pdb", "cif")
    return all(
        not mime_type.startswith(x) for x in ("image", "application/pdf", "text/plain")
    )


# This isn't really something the user should call. Rather, it's used in hyperlinks, so we don't include it in the schema.
@app.get("/api/files/{fileId}", tags=["files"], include_in_schema=False)
async def read_file(fileId: int, db: AsyncDB = Depends(get_db)) -> StreamingResponse:
    try:
        async with db.read_only_connection() as conn:
            file_ = await db.retrieve_file(conn, fileId, with_contents=True)

        def async_generator() -> Any:
            if file_.contents is not None:
                yield from BytesIO(file_.contents)

        # Content-Disposition makes it so the browser opens a "Save file as" dialog. For images, PDFs, ..., we can just
        # display them in the browser instead.
        return StreamingResponse(
            async_generator(),
            media_type=file_.type_,
            headers=(
                {"Content-Disposition": f'attachment; filename="{file_.file_name}"'}
                if _do_content_disposition(
                    file_.type_, Path(file_.file_name).suffix[1:]
                )
                else {}
            ),
        )
    except NotFoundError:
        raise HTTPException(status_code=404, detail=f"File with id {fileId} not found")


class JsonReadAttributi(BaseModel):
    attributi: list[JsonAttributo]


@app.get(
    "/api/attributi/{beamtimeId}",
    tags=["attributi"],
    response_model_exclude_defaults=True,
)
async def read_attributi(
    beamtimeId: BeamtimeId, db: AsyncDB = Depends(get_db)
) -> JsonReadAttributi:
    async with db.read_only_connection() as conn:
        return JsonReadAttributi(
            attributi=[
                _encode_attributo(a)
                for a in await db.retrieve_attributi(
                    conn, beamtimeId, associated_table=None
                )
            ]
        )


class JsonAnalysisDataSet(BaseModel):
    data_set: JsonDataSet
    runs: list[str]
    number_of_indexing_results: int
    merge_results: list[JsonMergeResult]


class JsonAnalysisExperimentType(BaseModel):
    experiment_type: int
    data_sets: list[JsonAnalysisDataSet]


class JsonReadAnalysisResults(BaseModel):
    attributi: list[JsonAttributo]
    chemical_id_to_name: list[JsonChemicalIdAndName]
    experiment_types: list[JsonExperimentType]
    data_sets: list[JsonAnalysisExperimentType]


@app.get(
    "/api/analysis/analysis-results/{beamtimeId}",
    tags=["analysis"],
    response_model_exclude_defaults=True,
)
async def read_analysis_results(
    beamtimeId: BeamtimeId, db: AsyncDB = Depends(get_db)
) -> JsonReadAnalysisResults:
    async with db.read_only_connection() as conn:
        attributi = await db.retrieve_attributi(conn, beamtimeId, associated_table=None)
        chemicals = await db.retrieve_chemicals(conn, beamtimeId, attributi)
        data_sets = await db.retrieve_data_sets(conn, beamtimeId, attributi)
        data_sets_by_experiment_type: dict[int, list[DBDataSet]] = group_by(
            data_sets,
            lambda ds: ds.experiment_type_id,
        )
        runs = {r.id: r for r in await db.retrieve_runs(conn, beamtimeId, attributi)}
        run_internal_to_external_id = {r.id: r.external_id for r in runs.values()}
        data_set_to_runs: dict[int, list[DBRunOutput]] = {
            ds.id: [
                r
                for r in runs.values()
                if r.experiment_type_id == ds.experiment_type_id
                and run_matches_dataset(r.attributi, ds.attributi)
            ]
            for ds in data_sets
        }
        # This is horrible, one SQL query could do the trick here. But efficiency comes second.
        indexing_results_for_runs: dict[int, list[DBIndexingResultOutput]] = group_by(
            await db.retrieve_indexing_results(conn, beamtime_id=beamtimeId),
            lambda ir: ir.run_id,
        )
        merge_results_per_data_set: dict[int, list[DBMergeResultOutput]] = {
            ds.id: [] for ds in data_sets
        }
        data_set_to_run_ids: dict[int, set[int]] = {
            ds_id: set(r.id for r in runs) for ds_id, runs in data_set_to_runs.items()
        }
        # Super hard to wrap your head around this, so let me explain:
        #
        # A merge result has a list of indexing results. Each indexing result belongs to exactly one run.
        # Thus, for each merge result, we can get a list of runs.
        #
        # A data set has a list of runs matching it.
        #
        # We now define: A merge result matches a data set if the data set's runs are a strict superset of the merge
        # result's runs.
        #
        # This is what's tested here.
        for merge_result in await db.retrieve_merge_results(
            conn, beamtime_id=beamtimeId
        ):
            runs_in_merge_result: set[int] = set(
                ir.run_id for ir in merge_result.indexing_results
            )
            for ds_id, run_ids in data_set_to_run_ids.items():
                if run_ids.issuperset(runs_in_merge_result):
                    merge_results_per_data_set[ds_id].append(merge_result)
        run_foms: dict[int, DBIndexingFOM] = {
            r.id: _indexing_fom_for_run(indexing_results_for_runs, r)
            for r in runs.values()
        }

        refinement_results_per_merge_result_id: dict[
            int, list[DBRefinementResultOutput]
        ] = group_by(
            await db.retrieve_refinement_results(conn, beamtimeId),
            lambda rr: rr.merge_result_id,
        )

        def _build_data_set_result(
            ds: DBDataSet, merge_results: list[DBMergeResultOutput]
        ) -> JsonAnalysisDataSet:
            runs_in_ds = data_set_to_runs.get(ds.id, [])
            return JsonAnalysisDataSet(
                data_set=_encode_data_set(
                    ds,
                    _summary_from_foms(
                        [
                            run_foms.get(r.id, empty_indexing_fom)
                            for r in runs.values()
                            if r.experiment_type_id == ds.experiment_type_id
                            and run_matches_dataset(r.attributi, ds.attributi)
                        ]
                    ),
                ),
                runs=format_run_id_intervals(r.external_id for r in runs_in_ds),
                number_of_indexing_results=sum(
                    len(indexing_results_for_runs.get(run.id, [])) for run in runs_in_ds
                ),
                merge_results=[
                    _encode_merge_result(
                        mr,
                        refinement_results_per_merge_result_id.get(mr.id, []),
                        run_id_formatter=lambda rid: run_internal_to_external_id[rid],
                    )
                    for mr in merge_results
                ],
            )

        return JsonReadAnalysisResults(
            attributi=[_encode_attributo(a) for a in attributi],
            chemical_id_to_name=[
                JsonChemicalIdAndName(chemical_id=x.id, name=x.name) for x in chemicals
            ],
            experiment_types=[
                _encode_experiment_type(et)
                for et in await db.retrieve_experiment_types(conn, beamtimeId)
            ],
            data_sets=[
                JsonAnalysisExperimentType(
                    experiment_type=et_id,
                    data_sets=[
                        _build_data_set_result(
                            ds, merge_results_per_data_set.get(ds.id, [])
                        )
                        for ds in data_sets
                    ],
                )
                for et_id, data_sets in data_sets_by_experiment_type.items()
            ],
        )


# Neat trick: mount this after the endpoints to have both static files under / (for example, /index.html) as well as API endpoints under /api/*
# Credits to https://stackoverflow.com/questions/65419794/serve-static-files-from-root-in-fastapi
real_static_folder = (
    hardcoded_static_folder
    if hardcoded_static_folder is not None
    else os.getcwd() + "/frontend/output"
)
if Path(real_static_folder).is_dir():
    app.mount(
        "/",
        StaticFiles(
            directory=real_static_folder,
            html=True,
        ),
        name="static",
    )
