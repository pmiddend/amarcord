import asyncio
import datetime
import json
import os
import sys
from dataclasses import replace
from io import BytesIO
from pathlib import Path
from statistics import mean
from tempfile import NamedTemporaryFile
from typing import Any
from typing import Final
from typing import Iterable
from typing import Set
from typing import cast
from zipfile import ZipFile

import quart
from hypercorn import Config
from hypercorn.asyncio import serve
from pint import UnitRegistry
from quart import Quart
from quart import redirect
from quart import request
from quart import send_file
from quart_cors import cors
from tap import Tap
from werkzeug import Response
from werkzeug.exceptions import HTTPException

from amarcord.amici.crystfel.util import CrystFELCellFile
from amarcord.amici.crystfel.util import coparse_cell_description
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.asyncdb import ATTRIBUTO_GROUP_MANUAL
from amarcord.db.asyncdb import LIVE_STREAM_IMAGE
from amarcord.db.asyncdb import MergeResultError
from amarcord.db.asyncdb import create_workbook
from amarcord.db.attributi import ATTRIBUTO_STARTED
from amarcord.db.attributi import ATTRIBUTO_STOPPED
from amarcord.db.attributi import AttributoConversionFlags
from amarcord.db.attributi import attributo_sort_key
from amarcord.db.attributi import attributo_type_to_schema
from amarcord.db.attributi import datetime_to_attributo_int
from amarcord.db.attributi import schema_to_attributo_type
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.attributi_map import JsonAttributiMap
from amarcord.db.attributi_map import convert_single_attributo_value_to_json
from amarcord.db.attributi_map import run_matches_dataset
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoTypeBoolean
from amarcord.db.data_set import DBDataSet
from amarcord.db.db_merge_result import DBMergeResultInput
from amarcord.db.db_merge_result import DBMergeResultOutput
from amarcord.db.db_merge_result import DBMergeRuntimeStatusDone
from amarcord.db.db_merge_result import DBMergeRuntimeStatusError
from amarcord.db.db_merge_result import DBMergeRuntimeStatusRunning
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.experiment_type import DBExperimentType
from amarcord.db.indexing_result import DBIndexingFOM
from amarcord.db.indexing_result import DBIndexingResultDone
from amarcord.db.indexing_result import DBIndexingResultOutput
from amarcord.db.indexing_result import DBIndexingResultRunning
from amarcord.db.indexing_result import empty_indexing_fom
from amarcord.db.merge_negative_handling import MergeNegativeHandling
from amarcord.db.schedule_entry import BeamtimeScheduleEntry
from amarcord.db.table_classes import DBChemical
from amarcord.db.table_classes import DBEvent
from amarcord.db.table_classes import DBFile
from amarcord.db.table_classes import DBRun
from amarcord.db.user_configuration import UserConfiguration
from amarcord.filter_expression import FilterInput
from amarcord.filter_expression import FilterParseError
from amarcord.filter_expression import compile_run_filter
from amarcord.json_checker import JSONChecker
from amarcord.json_schema import coparse_schema_type
from amarcord.json_schema import parse_schema_type
from amarcord.json_types import JSONDict
from amarcord.quart_utils import CustomJSONEncoder
from amarcord.quart_utils import CustomWebException
from amarcord.quart_utils import QuartDatabases
from amarcord.quart_utils import handle_exception
from amarcord.quart_utils import quart_safe_json_dict
from amarcord.util import create_intervals
from amarcord.util import group_by

AUTO_PILOT: Final = "auto-pilot"
ONLINE_CRYSTFEL: Final = "online-crystfel"
DATE_FORMAT: Final = "%Y-%m-%d"

hardcoded_static_folder: None | str = None

app = Quart(
    __name__,
    static_folder=hardcoded_static_folder
    if hardcoded_static_folder is not None
    else os.getcwd() + "/frontend/output",
    static_url_path="/",
)

app.json_encoder = CustomJSONEncoder
app = cors(app)
db = QuartDatabases(app)


@app.errorhandler(HTTPException)
def error_handler_for_exceptions(e: Any) -> Any:
    return handle_exception(e)


def format_run_id_intervals(run_ids: Iterable[int]) -> list[str]:
    return [
        str(t[0]) if t[0] == t[1] else f"{t[0]}-{t[1]}"
        for t in create_intervals(list(run_ids))
    ]


@app.post("/api/events")
async def create_event() -> JSONDict:
    r = JSONChecker(await quart_safe_json_dict(), "request")

    async with db.instance.begin() as conn:
        event = r.retrieve_safe_object("event")

        event_id = await db.instance.create_event(
            conn,
            level=EventLogLevel.INFO,
            source=cast(str, event["source"]),
            text=cast(str, event["text"]),
        )
        file_ids = cast(list[int], event["fileIds"])
        for file_id in file_ids:
            await db.instance.add_file_to_event(conn, file_id, event_id)
        if r.retrieve_safe_boolean("withLiveStream"):
            existing_live_stream = await db.instance.retrieve_file_id_by_name(
                conn, LIVE_STREAM_IMAGE
            )
            if existing_live_stream is not None:
                new_file_name = LIVE_STREAM_IMAGE + "-copy"
                duplicated_live_stream = await db.instance.duplicate_file(
                    conn, existing_live_stream, new_file_name
                )
                await db.instance.add_file_to_event(
                    conn, duplicated_live_stream.id, event_id
                )
        return {"id": event_id}


@app.get("/")
async def index() -> Response:
    return redirect("/index.html")


@app.get("/api/spreadsheet.zip")
async def download_spreadsheet() -> quart.wrappers.response.Response:
    async with db.instance.read_only_connection() as conn:
        workbook_output = await create_workbook(db.instance, conn, with_events=True)
        workbook = workbook_output.workbook
        workbook_bytes = BytesIO()
        workbook.save(workbook_bytes)
        zipfile_bytes = BytesIO()
        with ZipFile(zipfile_bytes, "w") as result_zip:
            result_zip.writestr("tables.xlsx", workbook_bytes.getvalue())
            for file_id in workbook_output.files:
                file_ = await db.instance.retrieve_file(
                    conn, file_id, with_contents=True
                )
                result_zip.writestr(
                    f"files/{file_id}" + Path(file_.file_name).suffix,
                    cast(bytes, file_.contents),
                )
        zipfile_bytes.seek(0)
        return await send_file(
            zipfile_bytes,
            "application/zip",
            attachment_filename="amarcord-output-"
            + datetime.datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
            + ".zip",
            as_attachment=True,
        )


@app.post("/api/chemicals")
async def create_chemical() -> JSONDict:
    r = JSONChecker(await quart_safe_json_dict(), "request")

    async with db.instance.begin() as conn:
        chemical_id = await db.instance.create_chemical(
            conn,
            name=r.retrieve_safe_str("name"),
            attributi=AttributiMap.from_types_and_json(
                await db.instance.retrieve_attributi(conn, AssociatedTable.CHEMICAL),
                chemical_ids=[],
                raw_attributi=r.retrieve_safe_object("attributi"),
            ),
        )
        file_ids = r.retrieve_int_array("fileIds")
        for file_id in file_ids:
            await db.instance.add_file_to_chemical(conn, file_id, chemical_id)

    return {"id": chemical_id}


@app.patch("/api/chemicals")
async def update_chemical() -> JSONDict:
    r = JSONChecker(await quart_safe_json_dict(), "request")

    async with db.instance.begin() as conn:
        chemical_id = r.retrieve_safe_int("id")
        attributi = await db.instance.retrieve_attributi(conn, AssociatedTable.CHEMICAL)
        chemical = await db.instance.retrieve_chemical(conn, chemical_id, attributi)
        assert chemical is not None
        chemical_attributi = chemical.attributi
        chemical_attributi.extend_with_attributi_map(
            AttributiMap.from_types_and_json(
                attributi,
                chemical_ids=[],
                raw_attributi=r.retrieve_safe_object("attributi"),
            )
        )
        await db.instance.update_chemical(
            conn,
            id_=chemical_id,
            name=r.retrieve_safe_str("name"),
            attributi=chemical_attributi,
        )
        await db.instance.remove_files_from_chemical(conn, chemical_id)
        file_ids = r.retrieve_int_array("fileIds")
        for file_id in file_ids:
            await db.instance.add_file_to_chemical(conn, file_id, chemical_id)

    return {}


@app.delete("/api/chemicals")
async def delete_chemical() -> JSONDict:
    r = JSONChecker(await quart_safe_json_dict(), "request")

    async with db.instance.begin() as conn:
        await db.instance.delete_chemical(
            conn, id_=r.retrieve_safe_int("id"), delete_in_dependencies=True
        )

    return {}


def _encode_file(f: DBFile) -> JSONDict:
    return {
        "id": f.id,
        "description": f.description,
        "type_": f.type_,
        "fileName": f.file_name,
        "sizeInBytes": f.size_in_bytes,
        "originalPath": f.original_path,
    }


def _encode_user_configuration(c: UserConfiguration) -> JSONDict:
    return {
        ONLINE_CRYSTFEL: c.use_online_crystfel,
        AUTO_PILOT: c.auto_pilot,
        "current-experiment-type-id": c.current_experiment_type_id,
    }


def _encode_chemical(a: DBChemical) -> JSONDict:
    return {
        "id": a.id,
        "name": a.name,
        "attributi": a.attributi.to_json(),
        "files": [_encode_file(f) for f in a.files],
    }


def _encode_merge_result(mr: DBMergeResultOutput) -> JSONDict:
    additional_dict: JSONDict = {}
    match mr.runtime_status:
        case None:
            additional_dict = {"state-queued": {}}
        case DBMergeRuntimeStatusRunning(job_id, started, recent_log):
            additional_dict = {
                "state-running": {
                    "started": datetime_to_attributo_int(started),
                    "job-id": job_id,
                    "latest-log": recent_log,
                }
            }
        case DBMergeRuntimeStatusError(error, started, stopped, recent_log):
            additional_dict = {
                "state-error": {
                    "started": datetime_to_attributo_int(started),
                    "stopped": datetime_to_attributo_int(stopped),
                    "error": error,
                    "latest-log": recent_log,
                }
            }
        case DBMergeRuntimeStatusDone(started, stopped, result):
            assert isinstance(
                result.mtz_file, int
            ), f"mtz file in result is not an integer but {result.mtz_file}"
            additional_dict = {
                "state-done": {
                    "started": datetime_to_attributo_int(started),
                    "stopped": datetime_to_attributo_int(stopped),
                    "result": {
                        "mtz-file-id": result.mtz_file,
                        "detailed-foms": [
                            {
                                "one-over-d-centre": i.one_over_d_centre,
                                "nref": i.nref,
                                "d-over-a": i.d_over_a,
                                "min-res": i.min_res,
                                "max-res": i.max_res,
                                "cc": i.cc,
                                "ccstar": i.ccstar,
                                "r-split": i.r_split,
                                "reflections-possible": i.reflections_possible,
                                "completeness": i.completeness,
                                "measurements": i.measurements,
                                "redundancy": i.redundancy,
                                "snr": i.snr,
                                "mean-i": i.mean_i,
                            }
                            for i in result.detailed_foms
                        ],
                        "fom": {
                            "snr": result.fom.snr,
                            "wilson": result.fom.wilson,
                            "ln-k": result.fom.ln_k,
                            "discarded-reflections": result.fom.discarded_reflections,
                            "one-over-d-from": result.fom.one_over_d_from,
                            "one-over-d-to": result.fom.one_over_d_to,
                            "redundancy": result.fom.redundancy,
                            "completeness": result.fom.completeness,
                            "measurements-total": result.fom.measurements_total,
                            "reflections-total": result.fom.reflections_total,
                            "reflections-possible": result.fom.reflections_possible,
                            "r-split": result.fom.r_split,
                            "r1i": result.fom.r1i,
                            "r2": result.fom.r2,
                            "cc": result.fom.cc,
                            "cc-star": result.fom.ccstar,
                            "cc-ano": result.fom.ccano,
                            "crd-ano": result.fom.crdano,
                            "r-ano": result.fom.rano,
                            "r-ano-over-r-split": result.fom.rano_over_r_split,
                            "d1sig": result.fom.d1sig,
                            "d2sig": result.fom.d2sig,
                            "outer-shell": {
                                "resolution": result.fom.outer_shell.resolution,
                                "cc-star": result.fom.outer_shell.ccstar,
                                "r-split": result.fom.outer_shell.r_split,
                                "cc": result.fom.outer_shell.cc,
                                "unique-reflections": result.fom.outer_shell.unique_reflections,
                                "completeness": result.fom.outer_shell.completeness,
                                "redundancy": result.fom.outer_shell.redundancy,
                                "snr": result.fom.outer_shell.snr,
                                "min-res": result.fom.outer_shell.min_res,
                                "max-res": result.fom.outer_shell.max_res,
                            },
                        },
                    },
                }
            }
    return {
        "id": mr.id,
        "created": datetime_to_attributo_int(mr.created),
        # We don't export the indexing results here yet. No clear reason other than laziness
        "runs": format_run_id_intervals(ir.run_id for ir in mr.indexing_results),
        "point-group": mr.point_group,
        "cell-description": coparse_cell_description(mr.cell_description),
        "partialator-additional": mr.partialator_additional,
        "negative-handling": mr.negative_handling.value
        if mr.negative_handling is not None
        else None,
    } | additional_dict


@app.get("/api/chemicals")
async def read_chemicals() -> JSONDict:
    async with db.instance.begin() as conn:
        attributi = await db.instance.retrieve_attributi(
            conn, associated_table=AssociatedTable.CHEMICAL
        )
        result: JSONDict = {
            "chemicals": [
                _encode_chemical(a)
                for a in await db.instance.retrieve_chemicals(conn, attributi)
            ],
            "attributi": [_encode_attributo(a) for a in attributi],
        }
        if _has_artificial_delay():
            await asyncio.sleep(3)
        return result


def _encode_event(e: DBEvent) -> JSONDict:
    return {
        "id": e.id,
        "text": e.text,
        "source": e.source,
        "created": datetime_to_attributo_int(e.created),
        "level": e.level.value,
        "files": [_encode_file(f) for f in e.files],
    }


def _has_artificial_delay() -> bool:
    return bool(app.config.get("ARTIFICIAL_DELAY", False))


@app.post("/api/merging/<int:data_set_id>/start")
async def start_merge_job_for_data_set(data_set_id: int) -> JSONDict:
    request_content = JSONChecker(await quart_safe_json_dict(), "request")

    async with db.instance.begin() as conn:
        strict_mode = request_content.retrieve_safe_boolean("strict-mode")
        attributi = await db.instance.retrieve_attributi(conn, associated_table=None)
        chemical_ids = await db.instance.retrieve_chemical_ids(conn)
        data_set = await db.instance.retrieve_data_set(
            conn, data_set_id, chemical_ids, attributi
        )
        if data_set is None:
            raise CustomWebException(
                code=404,
                title=f'Data set with ID "{data_set_id}" not found',
                description="",
            )
        runs = [
            run
            for run in await db.instance.retrieve_runs(
                conn,
                attributi,
            )
            if run_matches_dataset(run.attributi, data_set.attributi)
        ]
        if not runs:
            raise CustomWebException(
                code=500,
                title=f"Data set with ID {data_set_id} has no runs!",
                description="",
            )
        indexing_results_by_run_id: dict[int, list[DBIndexingResultOutput]] = group_by(
            (
                ir
                for ir in await db.instance.retrieve_indexing_results(conn)
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
                    raise CustomWebException(
                        code=404,
                        title=f"Run {run.id} has no indexing results and strict mode is on; cannot merge",
                        description="",
                    )
                continue
            chosen_result = irs[0]
            if chosen_result.cell_description is not None:
                cell_descriptions.add(chosen_result.cell_description)
            if chosen_result.point_group is not None:
                point_groups.add(chosen_result.point_group)
            chosen_indexing_results.append(chosen_result)
        if not chosen_indexing_results:
            raise CustomWebException(
                code=500,
                title="Found no indexing results for the runs "
                + ",".join(str(r.id) for r in runs),
                description="",
            )
        # Shouldn't happen, since for each indexing result we automatically have a cell description and point group, but
        # the type system is too clunky to express that easily.
        if not cell_descriptions:
            raise CustomWebException(
                code=500,
                title="Found no cell descriptions for the runs "
                + ",".join(str(r.id) for r in runs),
                description="",
            )
        if not point_groups:
            raise CustomWebException(
                code=500,
                title="Found no cell descriptions for the runs "
                + ",".join(str(r.id) for r in runs),
                description="",
            )
        if len(cell_descriptions) > 1:
            raise CustomWebException(
                code=500,
                title="Conflicting cell descriptions",
                description="We have more than one cell description and cannot merge: "
                + ", ".join(
                    coparse_cell_description(c)
                    for c in cell_descriptions
                    if c is not None
                ),
            )
        if len(point_groups) > 1:
            raise CustomWebException(
                code=500,
                title="Conflicting point groups",
                description="We have more than one point group and cannot merge: "
                + ", ".join(f for f in point_groups if f is not None),
            )
        negative_handling_str = request_content.optional_str("negative-handling")
        try:
            negative_handling = (
                MergeNegativeHandling(negative_handling_str)
                if negative_handling_str is not None
                else None
            )
        except:
            raise CustomWebException(
                code=500,
                title="Invalid value for negative-handling",
                description=f'Value "{negative_handling_str}" for negative-handling is not known',
            )
        merge_result_id = await db.instance.create_merge_result(
            conn,
            DBMergeResultInput(
                created=datetime.datetime.utcnow(),
                indexing_results=chosen_indexing_results,
                point_group=next(iter(point_groups)),
                cell_description=next(iter(cell_descriptions)),
                partialator_additional=request_content.retrieve_safe_str(
                    "partialator-additional"
                ),
                negative_handling=negative_handling,
                runtime_status=None,
            ),
        )
        if isinstance(merge_result_id, MergeResultError):
            raise CustomWebException(
                code=500, title=str(merge_result_id), description=""
            )
        return {"merge-result-id": merge_result_id}


@app.get("/api/runs/<int:run_id>/start")
async def start_run(run_id: int) -> JSONDict:
    async with db.instance.begin() as conn:
        attributi = await db.instance.retrieve_attributi(conn, AssociatedTable.RUN)

        json_attributi: JSONDict = {}
        if any(a.name == ATTRIBUTO_STARTED for a in attributi):
            json_attributi[ATTRIBUTO_STARTED] = datetime.datetime.utcnow()

        await db.instance.create_run(
            conn,
            run_id,
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_raw(
                types=attributi,
                chemical_ids=[],
                raw_attributi={ATTRIBUTO_STARTED: datetime.datetime.utcnow()},
            ),
            keep_manual_attributes_from_previous_run=True,
        )
        return {}


@app.get("/api/runs/stop-latest")
async def stop_latest_run() -> JSONDict:
    async with db.instance.begin() as conn:
        attributi = await db.instance.retrieve_attributi(conn, AssociatedTable.RUN)

        if not any(a.name for a in attributi):
            return {}

        latest_run = await db.instance.retrieve_latest_run(conn, attributi)

        if latest_run is not None:
            latest_run.attributi.append_single(
                ATTRIBUTO_STOPPED, datetime.datetime.utcnow()
            )
            await db.instance.update_run_attributi(
                conn, latest_run.id, latest_run.attributi
            )

        return {}


@app.patch("/api/runs")
async def update_run() -> JSONDict:
    r = JSONChecker(await quart_safe_json_dict(), "request")

    async with db.instance.begin() as conn:
        run_id = r.retrieve_safe_int("id")
        attributi = await db.instance.retrieve_attributi(conn, AssociatedTable.RUN)
        current_run = await db.instance.retrieve_run(conn, run_id, attributi)
        if current_run is None:
            raise CustomWebException(
                code=404,
                title=f"Couldn't find run {run_id}, was it deleted?",
                description="",
            )
        chemical_ids = await db.instance.retrieve_chemical_ids(conn)
        raw_attributi = r.retrieve_safe_object("attributi")
        current_run.attributi.extend_with_attributi_map(
            AttributiMap.from_types_and_json(attributi, chemical_ids, raw_attributi)
        )
        await db.instance.update_run_attributi(
            conn,
            id_=run_id,
            attributi=current_run.attributi,
        )

    return {}


def _summary_from_foms(ir: list[DBIndexingFOM]) -> DBIndexingFOM:
    if not ir:
        return empty_indexing_fom
    return DBIndexingFOM(
        hit_rate=mean(x.hit_rate for x in ir),
        indexing_rate=mean(x.indexing_rate for x in ir),
        indexed_frames=sum(x.indexed_frames for x in ir),
    )


@app.post("/api/runs/bulk")
async def read_runs_bulk() -> JSONDict:
    r = JSONChecker(await quart_safe_json_dict(), "request")

    async with db.instance.read_only_connection() as conn:
        attributi = await db.instance.retrieve_attributi(conn, associated_table=None)
        chemicals = await db.instance.retrieve_chemicals(conn, attributi)
        return {
            "chemicals": [_encode_chemical(s) for s in chemicals],
            "attributi": [
                _encode_attributo(a)
                for a in attributi
                if a.name not in (ATTRIBUTO_STOPPED, ATTRIBUTO_STARTED)
                and a.associated_table == AssociatedTable.RUN
            ],
            "attributi-map": {
                attributo_id: [
                    convert_single_attributo_value_to_json(v) for v in values
                ]
                for attributo_id, values in (
                    await db.instance.retrieve_bulk_run_attributi(
                        conn,
                        attributi,
                        r.retrieve_int_array("run-ids"),
                    )
                ).items()
            },
        }


@app.patch("/api/runs/bulk")
async def update_runs_bulk() -> JSONDict:
    r = JSONChecker(await quart_safe_json_dict(), "request")

    async with db.instance.begin() as conn:
        attributi = await db.instance.retrieve_attributi(conn, associated_table=None)
        await db.instance.update_bulk_run_attributi(
            conn,
            attributi,
            set(r.retrieve_int_array("run-ids")),
            AttributiMap.from_types_and_json(
                attributi,
                await db.instance.retrieve_chemical_ids(conn),
                r.retrieve_safe_object("attributi"),
            ),
        )
        return {}


def extract_runs_and_event_dates(runs: list[DBRun], events: list[DBEvent]) -> list[str]:
    set_of_dates: Set[str] = set()
    for run in runs:
        started_date = run.attributi.select_datetime(ATTRIBUTO_STARTED)
        if started_date is not None:
            set_of_dates.add(started_date.strftime(DATE_FORMAT))
    for event in events:
        set_of_dates.add(event.created.strftime(DATE_FORMAT))

    return sorted(set_of_dates, reverse=True)


def run_has_started_date(run: DBRun, date_filter: str) -> bool:
    started = run.attributi.select_datetime(ATTRIBUTO_STARTED)
    if started is not None:
        return started.strftime(DATE_FORMAT) == date_filter
    return True


def event_has_date(event: DBEvent, date_filter: str) -> bool:
    return event.created.strftime(DATE_FORMAT) == date_filter


@app.get("/api/runs")
async def read_runs() -> JSONDict:
    async with db.instance.read_only_connection() as conn:
        attributi = await db.instance.retrieve_attributi(conn, associated_table=None)
        attributi.sort(key=attributo_sort_key)
        chemicals = await db.instance.retrieve_chemicals(conn, attributi)
        experiment_types = await db.instance.retrieve_experiment_types(conn)
        data_sets = await db.instance.retrieve_data_sets(
            conn, [s.id for s in chemicals], attributi
        )
        all_runs = await db.instance.retrieve_runs(conn, attributi)
        all_events = await db.instance.retrieve_events(conn)

        try:
            run_filter = compile_run_filter(request.args.get("filter", ""))
            runs = [
                run
                for run in all_runs
                if run_filter(
                    FilterInput(
                        run=run, chemical_names={s.name: s.id for s in chemicals}
                    )
                )
            ]
        except FilterParseError as e:
            raise CustomWebException(
                code=200, title="Error in filter string", description=str(e)
            )

        date_filter = request.args.get("date", "").strip()
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

        indexing_results = await db.instance.retrieve_indexing_results(conn)
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
                    if run_matches_dataset(r.attributi, ds.attributi)
                ]
            )
            for ds in data_sets
        }

        user_configuration = await db.instance.retrieve_configuration(conn)
        result: JSONDict = {
            "live-stream-file-id": await db.instance.retrieve_file_id_by_name(
                conn, LIVE_STREAM_IMAGE
            ),
            "filter-dates": extract_runs_and_event_dates(all_runs, all_events),
            "runs": [
                {
                    "id": r.id,
                    "attributi": r.attributi.to_json(),
                    "files": [_encode_file(f) for f in r.files],
                    "summary": _encode_summary(run_foms.get(r.id, empty_indexing_fom)),
                    "data-sets": [
                        ds.id
                        for ds in data_sets
                        if run_matches_dataset(r.attributi, ds.attributi)
                    ],
                    "running-indexing-jobs": [
                        ir.id
                        for ir in indexing_results
                        if ir.run_id == r.id
                        and isinstance(ir.runtime_status, DBIndexingResultRunning)
                    ],
                }
                for r in runs
            ],
            "attributi": [_encode_attributo(a) for a in attributi],
            "experiment-types": [_encode_experiment_type(a) for a in experiment_types],
            "data-sets": [
                _encode_data_set(a, data_set_id_to_grouped.get(a.id, None))
                for a in data_sets
            ],
            "events": [_encode_event(e) for e in events],
            "chemicals": [_encode_chemical(a) for a in chemicals],
            "user-config": _encode_user_configuration(user_configuration),
        }
        if _has_artificial_delay():
            await asyncio.sleep(3)
        return result


@app.post("/api/files")
async def create_file() -> JSONDict:
    f = await request.form
    files = await request.files

    assert f, "request form was empty, need some metadata!"
    assert files, "Koalas in the rain, no files given"

    # We expect a request with two parts: one with just a JSON key-value pair and one file
    r = JSONChecker(json.loads(next(f.values())), "request")
    description = r.retrieve_safe_str("description")

    async with db.instance.begin() as conn:
        file = next(files.values())
        file_name = file.filename

        # Since we potentially need to seek around in the file, and we don't know if it's a seekable
        # stream (I think?) we store it in a named temp file first.
        with NamedTemporaryFile(mode="w+b") as temp_file:
            temp_file.write(file.read())
            temp_file.flush()
            temp_file.seek(0, os.SEEK_SET)

            create_result = await db.instance.create_file(
                conn,
                file_name=file_name,
                description=description,
                original_path=None,
                contents_location=Path(temp_file.name),
                deduplicate=False,
            )

    return {
        "id": create_result.id,
        "fileName": file_name,
        "description": description,
        "type_": create_result.type_,
        "sizeInBytes": create_result.size_in_bytes,
        # Doesn't really make sense here
        "originalPath": None,
    }


@app.get("/api/user-config/<key>")
async def read_user_configuration_single(key: str) -> JSONDict:
    async with db.instance.read_only_connection() as conn:
        user_configuration = await db.instance.retrieve_configuration(conn)
        if key == AUTO_PILOT:
            return {"value": user_configuration.auto_pilot}
        if key == ONLINE_CRYSTFEL:
            return {"value": user_configuration.use_online_crystfel}
        raise CustomWebException(
            code=400,
            title=f'Invalid key "{key}"',
            description=f'Couldn\'t find config key {key}, only know "{AUTO_PILOT}", "{ONLINE_CRYSTFEL}"!',
        )


@app.patch("/api/user-config/<key>/<value>")
async def update_user_configuration_single(key: str, value: str) -> JSONDict:
    async with db.instance.begin() as conn:
        user_configuration = await db.instance.retrieve_configuration(conn)
        if key == AUTO_PILOT:
            new_value = value == "True"

            new_configuration = replace(
                user_configuration,
                auto_pilot=new_value,
            )
            await db.instance.update_configuration(
                conn,
                new_configuration,
            )
            return {"value": new_value}
        if key == ONLINE_CRYSTFEL:
            new_value = value == "True"

            new_configuration = replace(
                user_configuration,
                use_online_crystfel=new_value,
            )
            await db.instance.update_configuration(
                conn,
                new_configuration,
            )
            return {"value": new_value}
        raise CustomWebException(
            code=400,
            title=f'Invalid key "{key}"',
            description=f'Couldn\'t find config key {key}, only know "{AUTO_PILOT}"!',
        )


@app.post("/api/experiment-types")
async def create_experiment_type() -> JSONDict:
    r = JSONChecker(await quart_safe_json_dict(), "request")

    async with db.instance.begin() as conn:
        et_id = await db.instance.create_experiment_type(
            conn,
            name=r.retrieve_safe_str("name"),
            experiment_attributi_names=r.retrieve_string_array("attributi-names"),
        )

    return {"id": et_id}


@app.post("/api/experiment-types/change-for-run")
async def change_current_run_experiment_type() -> JSONDict:
    r = JSONChecker(await quart_safe_json_dict(), "request")

    async with db.instance.begin() as conn:
        run_id = r.retrieve_safe_int("run-id")
        attributi = await db.instance.retrieve_attributi(conn, None)
        run = await db.instance.retrieve_run(
            conn,
            run_id,
            attributi,
        )
        if run is None:
            raise CustomWebException(
                code=404, title=f'Couldn\'t find run with ID "{run_id}"', description=""
            )
        new_experiment_type = r.optional_int("experiment-type-id")
        # There is no semantic yet for resetting an experiment type
        if new_experiment_type is None:
            return {}

        experiment_type_instance = next(
            iter(
                x
                for x in await db.instance.retrieve_experiment_types(conn)
                if x.id == new_experiment_type
            ),
            None,
        )
        if experiment_type_instance is None:
            raise CustomWebException(
                code=404,
                title=f"Couldn't find experiment type with ID {new_experiment_type}",
                description="",
            )
        for a in attributi:
            if (
                a.group == ATTRIBUTO_GROUP_MANUAL
                and a.name not in experiment_type_instance.attributi_names
            ):
                run.attributi.remove_but_keep_type(a.name)
        await db.instance.update_run_attributi(conn, run_id, run.attributi)
        await db.instance.update_configuration(
            conn,
            replace(
                await db.instance.retrieve_configuration(conn),
                current_experiment_type_id=new_experiment_type,
            ),
        )
        return {}


def _encode_experiment_type(a: DBExperimentType) -> JSONDict:
    return {
        "id": a.id,
        "name": a.name,
        "attributi-names": a.attributi_names,
    }


@app.get("/api/experiment-types")
async def read_experiment_types() -> JSONDict:
    async with db.instance.read_only_connection() as conn:
        if _has_artificial_delay():
            await asyncio.sleep(3)
        return {
            "experiment-types": [
                _encode_experiment_type(a)
                for a in await db.instance.retrieve_experiment_types(conn)
            ],
            "attributi": [
                _encode_attributo(a)
                for a in await db.instance.retrieve_attributi(
                    conn, associated_table=AssociatedTable.RUN
                )
            ],
        }


@app.delete("/api/experiment-types")
async def delete_experiment_type() -> JSONDict:
    r = JSONChecker(await quart_safe_json_dict(), "request")

    async with db.instance.begin() as conn:
        await db.instance.delete_experiment_type(
            conn,
            r.retrieve_safe_int("id"),
        )

    return {}


@app.get("/api/schedule")
async def get_beamtime_schedule() -> JSONDict:
    async with db.instance.read_only_connection() as conn:
        schedule = await db.instance.retrieve_beamtime_schedule(conn)
        return {"schedule": schedule}


@app.post("/api/schedule")
async def update_beamtime_schedule() -> JSONDict:
    request_json = JSONChecker(await quart_safe_json_dict(), "request")
    async with db.instance.begin() as conn:
        await db.instance.replace_beamtime_schedule(
            conn=conn,
            schedule=[
                BeamtimeScheduleEntry(
                    users=shift_dict.get("users", None),
                    date=shift_dict.get("date", None),
                    shift=shift_dict.get("shift", None),
                    comment=shift_dict.get("comment", ""),
                    td_support=shift_dict.get("td_support", ""),
                    chemicals=shift_dict.get("chemicals", []),
                )
                for shift_dict in request_json.retrieve_safe_list("schedule")
            ],
        )

    async with db.instance.begin() as conn:
        schedule = await db.instance.retrieve_beamtime_schedule(conn)
        return {"schedule": schedule}


@app.get("/api/live-stream/snapshot")
async def create_live_stream_snapshot() -> JSONDict:
    async with db.instance.begin() as conn:
        existing_live_stream = await db.instance.retrieve_file_id_by_name(
            conn, LIVE_STREAM_IMAGE
        )
        if existing_live_stream is None:
            raise CustomWebException(
                code=500,
                title="No live stream image found",
                description="Cannot make a snapshot of a non-existing image!",
            )
        new_file_name = LIVE_STREAM_IMAGE + "-copy"
        new_image = await db.instance.duplicate_file(
            conn, existing_live_stream, new_file_name
        )
        return {
            "id": new_image.id,
            "fileName": new_file_name,
            "description": "",
            "type_": new_image.type_,
            "sizeInBytes": new_image.size_in_bytes,
            # Doesn't really make sense here
            "originalPath": None,
        }


@app.post("/api/live-stream")
async def update_live_stream() -> JSONDict:
    files = await request.files

    assert files, "Koalas in the rain, no files given"

    async with db.instance.begin() as conn:
        file = next(files.values())

        # Since we potentially need to seek around in the file, and we don't know if it's a seekable
        # stream (I think?) we store it in a named temp file first.
        with NamedTemporaryFile(mode="w+b") as temp_file:
            temp_file.write(file.read())
            temp_file.flush()
            temp_file.seek(0, os.SEEK_SET)

            existing_live_stream = await db.instance.retrieve_file_id_by_name(
                conn, LIVE_STREAM_IMAGE
            )

            file_id: int
            if existing_live_stream is not None:
                file_id = existing_live_stream
                await db.instance.update_file(
                    conn, existing_live_stream, contents_location=Path(temp_file.name)
                )
            else:
                file_id = (
                    await db.instance.create_file(
                        conn,
                        file_name=LIVE_STREAM_IMAGE,
                        description="Live stream image",
                        original_path=None,
                        contents_location=Path(temp_file.name),
                        deduplicate=False,
                    )
                ).id

            return {"id": file_id}


@app.post("/api/data-sets/from-run")
async def create_data_set_from_run() -> JSONDict:
    r = JSONChecker(await quart_safe_json_dict(), "request")

    async with db.instance.begin() as conn:
        run_id = r.retrieve_safe_int("run-id")
        experiment_type_id = r.retrieve_safe_int("experiment-type-id")

        experiment_type_resolved = next(
            iter(
                et
                for et in await db.instance.retrieve_experiment_types(conn)
                if et.id == experiment_type_id
            ),
            None,
        )

        if experiment_type_resolved is None:
            raise CustomWebException(
                code=500,
                title=f"Couldn't find experiment type with ID {experiment_type_id}",
                description="",
            )

        attributi = await db.instance.retrieve_attributi(conn, AssociatedTable.RUN)

        run = await db.instance.retrieve_run(conn, run_id, attributi)

        if run is None:
            raise CustomWebException(
                code=500,
                title=f"Couldn't find run \"{run_id}",
                description="",
            )

        attributi_map = AttributiMap(
            {a.name: a for a in attributi},
            await db.instance.retrieve_chemical_ids(conn),
            {
                an: run.attributi.select(an)
                for an in experiment_type_resolved.attributi_names
            },
        )

        await db.instance.create_data_set(conn, experiment_type_id, attributi_map)

        return {}


@app.post("/api/data-sets")
async def create_data_set() -> JSONDict:
    r = JSONChecker(await quart_safe_json_dict(), "request")

    async with db.instance.begin() as conn:
        attributi = await db.instance.retrieve_attributi(conn, associated_table=None)
        attributi_by_name: dict[str, DBAttributo] = {a.name: a for a in attributi}
        chemical_ids = await db.instance.retrieve_chemical_ids(conn)
        previous_data_sets = await db.instance.retrieve_data_sets(
            conn, chemical_ids, attributi
        )

        experiment_type_id = r.retrieve_safe_int("experiment-type-id")
        experiment_type = next(
            iter(
                t
                for t in await db.instance.retrieve_experiment_types(conn)
                if t.id == experiment_type_id
            ),
            None,
        )

        if experiment_type is None:
            raise CustomWebException(
                code=500,
                title="Invalid experiment type",
                description=f"Experiment type with ID {experiment_type_id} not found",
            )

        data_set_attributi = r.retrieve_safe_object("attributi")
        if any(
            x
            for x in previous_data_sets
            if x.experiment_type_id == experiment_type_id
            and x.attributi.to_json() == data_set_attributi
        ):
            raise CustomWebException(
                code=500,
                title="Data set duplicate",
                description="This data set already exists!",
            )

        if not data_set_attributi:
            raise CustomWebException(
                code=500,
                title="Invalid data set",
                description="You have to set a least one attributo value",
            )

        processed_attributi: JsonAttributiMap = {}
        for attributo_name in experiment_type.attributi_names:
            if attributo_name in data_set_attributi:
                processed_attributi[attributo_name] = data_set_attributi[attributo_name]
            else:
                attributo = attributi_by_name.get(attributo_name, None)
                assert (
                    attributo is not None
                ), f"attributo {attributo_name} mentioned in experiment type {experiment_type_id} not found"
                if not isinstance(attributo.attributo_type, AttributoTypeBoolean):
                    raise CustomWebException(
                        code=500,
                        title="Invalid input",
                        description=f'Got no value for attributo "{attributo_name}"',
                    )
                processed_attributi[attributo_name] = False

        data_set_id = await db.instance.create_data_set(
            conn,
            experiment_type_id=experiment_type_id,
            attributi=AttributiMap.from_types_and_json(
                attributi,
                chemical_ids,
                processed_attributi,
            ),
        )

    return {"id": data_set_id}


def _encode_data_set(a: DBDataSet, summary: DBIndexingFOM | None) -> JSONDict:
    result = {
        "id": a.id,
        "experiment-type-id": a.experiment_type_id,
        "attributi": a.attributi.to_json(),
    }
    if summary is not None:
        result["summary"] = _encode_summary(summary)
    return result


def _encode_summary(summary: DBIndexingFOM) -> JSONDict:
    return {
        "hit-rate": summary.hit_rate,
        "indexing-rate": summary.indexing_rate,
        "indexed-frames": summary.indexed_frames,
    }


@app.get("/api/data-sets")
async def read_data_sets() -> JSONDict:
    async with db.instance.read_only_connection() as conn:
        if _has_artificial_delay():
            await asyncio.sleep(3)
        attributi = list(
            await db.instance.retrieve_attributi(conn, associated_table=None)
        )
        chemicals = await db.instance.retrieve_chemicals(conn, attributi)
        experiment_types = await db.instance.retrieve_experiment_types(conn)
        return {
            "data-sets": [
                _encode_data_set(a, summary=None)
                for a in await db.instance.retrieve_data_sets(
                    conn,
                    [s.id for s in chemicals],
                    attributi,
                )
            ],
            "chemicals": [_encode_chemical(s) for s in chemicals],
            "attributi": [_encode_attributo(a) for a in attributi],
            "experiment-types": [_encode_experiment_type(a) for a in experiment_types],
        }


@app.delete("/api/data-sets")
async def delete_data_set() -> JSONDict:
    r = JSONChecker(await quart_safe_json_dict(), "request")

    async with db.instance.begin() as conn:
        await db.instance.delete_data_set(
            conn,
            id_=r.retrieve_safe_int("id"),
        )

    return {}


@app.delete("/api/events")
async def delete_event() -> JSONDict:
    r = JSONChecker(await quart_safe_json_dict(), "request")

    async with db.instance.begin() as conn:
        await db.instance.delete_event(conn, r.retrieve_safe_int("id"))

    return {}


@app.delete("/api/files")
async def delete_file() -> JSONDict:
    r = JSONChecker(await quart_safe_json_dict(), "request")

    async with db.instance.begin() as conn:
        await db.instance.delete_file(conn, r.retrieve_safe_int("id"))

    return {}


@app.post("/api/attributi")
async def create_attributo() -> JSONDict:
    r = JSONChecker(await quart_safe_json_dict(), "request")

    async with db.instance.begin() as conn:
        await db.instance.create_attributo(
            conn,
            name=r.retrieve_safe_str("name"),
            description=r.retrieve_safe_str("description"),
            group=r.retrieve_safe_str("group"),
            associated_table=AssociatedTable(r.retrieve_safe_str("associatedTable")),
            type_=schema_to_attributo_type(parse_schema_type(r.retrieve_safe("type"))),
        )

    return {}


@app.patch("/api/attributi")
async def update_attributo() -> JSONDict:
    r = JSONChecker(await quart_safe_json_dict(), "request")

    async with db.instance.begin() as conn:
        new_attributo_json = JSONChecker(
            r.retrieve_safe("newAttributo"), "newAttributo"
        )
        conversion_flags = JSONChecker(
            r.retrieve_safe("conversionFlags"), "conversionFlags"
        )
        new_attributo = DBAttributo(
            name=AttributoId(new_attributo_json.retrieve_safe_str("name")),
            description=new_attributo_json.retrieve_safe_str("description"),
            group=new_attributo_json.retrieve_safe_str("group"),
            associated_table=AssociatedTable(
                new_attributo_json.retrieve_safe_str("associatedTable")
            ),
            attributo_type=schema_to_attributo_type(
                parse_schema_type(new_attributo_json.retrieve_safe("type"))
            ),
        )
        await db.instance.update_attributo(
            conn,
            name=AttributoId(r.retrieve_safe_str("nameBefore")),
            conversion_flags=AttributoConversionFlags(
                ignore_units=conversion_flags.retrieve_safe_boolean("ignoreUnits")
            ),
            new_attributo=new_attributo,
        )

    return {}


@app.post("/api/unit")
async def check_standard_unit() -> JSONDict:
    r = JSONChecker(await quart_safe_json_dict(), "request")

    unit_input = r.retrieve_safe_str("input")

    if unit_input == "":
        return {"input": unit_input, "error": "Unit empty"}

    try:
        return {"input": unit_input, "normalized": f"{UnitRegistry()(unit_input):P}"}
    except:
        return {"input": unit_input, "error": "Invalid unit"}


@app.delete("/api/attributi")
async def delete_attributo() -> JSONDict:
    r = JSONChecker(await quart_safe_json_dict(), "request")

    async with db.instance.begin() as conn:
        attributo_name = r.retrieve_safe_str("name")

        attributi = await db.instance.retrieve_attributi(conn, associated_table=None)

        found_attributo = next((x for x in attributi if x.name == attributo_name), None)
        if found_attributo is None:
            raise Exception(f"couldn't find attributo {found_attributo}")

        await db.instance.delete_attributo(
            conn,
            name=AttributoId(attributo_name),
        )

        if found_attributo.associated_table == AssociatedTable.CHEMICAL:
            for s in await db.instance.retrieve_chemicals(conn, attributi):
                s.attributi.remove_with_type(AttributoId(attributo_name))
                await db.instance.update_chemical(conn, s.id, s.name, s.attributi)
        else:
            for run in await db.instance.retrieve_runs(conn, attributi):
                run.attributi.remove_with_type(AttributoId(attributo_name))
                await db.instance.update_run_attributi(conn, run.id, run.attributi)

    return {}


def _encode_attributo(a: DBAttributo) -> JSONDict:
    return {
        "name": a.name,
        "description": a.description,
        "group": a.group,
        "associatedTable": a.associated_table.value,
        "type": coparse_schema_type(attributo_type_to_schema(a.attributo_type)),
    }


@app.get("/api/files/<int:file_id>")
async def read_file(file_id: int) -> Any:
    async with db.instance.read_only_connection() as conn:
        file_ = await db.instance.retrieve_file(conn, file_id, with_contents=True)

    async def async_generator() -> Any:
        yield file_.contents

    headers = {"Content-Type": file_.type_}
    # Content-Disposition makes it so the browser opens a "Save file as" dialog. For images, PDFs, ..., we can just
    # display them in the browser instead.
    dont_do_disposition = ("image", "application/pdf", "text/plain")
    if all(not file_.type_.startswith(x) for x in dont_do_disposition):
        headers["Content-Disposition"] = f'attachment; filename="{file_.file_name}"'
    return async_generator(), 200, headers


@app.get("/api/attributi")
async def read_attributi() -> JSONDict:
    async with db.instance.read_only_connection() as conn:
        if _has_artificial_delay():
            await asyncio.sleep(3)
        return {
            "attributi": [
                _encode_attributo(a)
                for a in await db.instance.retrieve_attributi(
                    conn, associated_table=None
                )
            ]
        }


@app.get("/api/config")
async def read_config() -> JSONDict:
    return {"title": app.config["TITLE"]}


def _build_summary_for_runs(
    run_list: list[DBRun],
    indexing_results_for_runs: dict[int, list[DBIndexingResultOutput]],
) -> DBIndexingFOM:
    indexing_results: list[DBIndexingFOM] = []
    for run in run_list:
        fom_for_run = _indexing_fom_for_run(indexing_results_for_runs, run)
        if fom_for_run is not None:
            indexing_results.append(fom_for_run)

    if indexing_results:
        return _summary_from_foms(indexing_results)
    return empty_indexing_fom


def _indexing_fom_for_run(
    indexing_results_for_runs: dict[int, list[DBIndexingResultOutput]], run: DBRun
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


@app.get("/api/analysis/analysis-results")
async def read_analysis_results() -> JSONDict:
    async with db.instance.read_only_connection() as conn:
        attributi = await db.instance.retrieve_attributi(conn, associated_table=None)
        chemicals = await db.instance.retrieve_chemicals(conn, attributi)
        data_sets = await db.instance.retrieve_data_sets(
            conn, [x.id for x in chemicals], attributi
        )
        data_sets_by_experiment_type: dict[int, list[DBDataSet]] = group_by(
            data_sets,
            lambda ds: ds.experiment_type_id,
        )
        runs = {r.id: r for r in await db.instance.retrieve_runs(conn, attributi)}
        data_set_to_runs: dict[int, list[DBRun]] = {
            ds.id: [
                r
                for r in runs.values()
                if run_matches_dataset(r.attributi, ds.attributi)
            ]
            for ds in data_sets
        }
        # This is horrible, one SQL query could do the trick here. But efficiency comes second.
        indexing_results_for_runs: dict[int, list[DBIndexingResultOutput]] = group_by(
            await db.instance.retrieve_indexing_results(conn), lambda ir: ir.run_id
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
        for merge_result in await db.instance.retrieve_merge_results(conn):
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

        def _build_data_set_result(
            ds: DBDataSet, merge_results: list[DBMergeResultOutput]
        ) -> JSONDict:
            runs_in_ds = data_set_to_runs.get(ds.id, [])
            return {
                "data-set": _encode_data_set(
                    ds,
                    _summary_from_foms(
                        [
                            run_foms.get(r.id, empty_indexing_fom)
                            for r in runs.values()
                            if run_matches_dataset(r.attributi, ds.attributi)
                        ]
                    ),
                ),
                "runs": format_run_id_intervals(r.id for r in runs_in_ds),
                "number-of-indexing-results": sum(
                    len(indexing_results_for_runs.get(run.id, [])) for run in runs_in_ds
                ),
                "merge-results": [_encode_merge_result(mr) for mr in merge_results],
            }

        return {
            "attributi": [_encode_attributo(a) for a in attributi],
            "chemical-id-to-name": [[x.id, x.name] for x in chemicals],
            "experiment-types": [
                _encode_experiment_type(et)
                for et in await db.instance.retrieve_experiment_types(conn)
            ],
            "data-sets": [
                {
                    "experiment-type": et_id,
                    "data-sets": [
                        _build_data_set_result(
                            ds, merge_results_per_data_set.get(ds.id, [])
                        )
                        for ds in data_sets
                    ],
                }
                for et_id, data_sets in data_sets_by_experiment_type.items()
            ],
        }


class Arguments(Tap):
    port: int = 5000
    host: str = "localhost"
    db_connection_url: str = "sqlite+aiosqlite://"
    db_echo: bool = False
    debug: bool = False
    delay: bool = False
    title: str = "AMARCORD"


def parse_args_from_env_vars() -> Arguments:
    args = Arguments()
    host = os.environ.get("AMARCORD_HOST", None)
    if host is not None:
        args.host = host
    else:
        args.host = "0.0.0.0"
    port = os.environ.get("AMARCORD_PORT", None)
    if port is not None:
        args.port = int(port)
    else:
        args.port = 5000
    url = os.environ.get("AMARCORD_DB_CONNECTION_URL", None)
    if url is not None:
        args.db_connection_url = url
    else:
        args.db_connection_url = "sqlite+aiosqlite://"
    return args


def main() -> int:
    # For debugging purposes, we don't want to use command line arguments, since hypercorn (which belongs to quart)
    # cannot hot-reload when you give it command line parameters. So we also use env vars.
    if "AMARCORD_DB_CONNECTION_URL" in os.environ:
        args = parse_args_from_env_vars()
    else:
        args = Arguments(underscores_to_dashes=True).parse_args()
    app.config.update(
        {
            "DB_URL": args.db_connection_url,
            "DB_ECHO": args.db_echo,
            "HAS_ARTIFICIAL_DELAY": args.delay,
            "TITLE": args.title,
        },
    )
    if args.debug:
        app.run(
            host=args.host, port=args.port, debug=args.debug, use_reloader=args.debug
        )
    else:
        config = Config()
        config.bind = [f"{args.host}:{args.port}"]
        asyncio.run(serve(app, config))
    return 0


if __name__ == "__main__":
    sys.exit(main())
