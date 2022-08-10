import asyncio
import datetime
import json
import os
import sys
from dataclasses import dataclass
from dataclasses import replace
from io import BytesIO
from pathlib import Path
from statistics import mean
from tempfile import NamedTemporaryFile
from typing import Any
from typing import Final
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

from amarcord.amici.om.client import ATTRIBUTO_HIT_RATE
from amarcord.amici.xfel.karabo_bridge import ATTRIBUTO_ID_DARK_RUN_TYPE
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.asyncdb import ATTRIBUTO_GROUP_MANUAL
from amarcord.db.asyncdb import LIVE_STREAM_IMAGE
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
from amarcord.db.cfel_analysis_result import DBCFELAnalysisResult
from amarcord.db.data_set import DBDataSet
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.experiment_type import DBExperimentType
from amarcord.db.table_classes import DBEvent
from amarcord.db.table_classes import DBFile
from amarcord.db.table_classes import DBRun
from amarcord.db.table_classes import DBSample
from amarcord.experiment_simulator import ATTRIBUTO_TARGET_FRAME_COUNT
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
DATE_FORMAT: Final = "%Y-%m-%d"

app = Quart(
    __name__,
    static_folder=os.environ.get(
        "AMARCORD_STATIC_FOLDER", os.getcwd() + "/frontend/output"
    ),
    static_url_path="/",
)

app.json_encoder = CustomJSONEncoder
app = cors(app)
db = QuartDatabases(app)


@app.errorhandler(HTTPException)
def error_handler_for_exceptions(e: Any) -> Any:
    return handle_exception(e)


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


@app.post("/api/samples")
async def create_sample() -> JSONDict:
    r = JSONChecker(await quart_safe_json_dict(), "request")

    async with db.instance.begin() as conn:
        sample_id = await db.instance.create_sample(
            conn,
            name=r.retrieve_safe_str("name"),
            attributi=AttributiMap.from_types_and_json(
                await db.instance.retrieve_attributi(conn, AssociatedTable.SAMPLE),
                sample_ids=[],
                raw_attributi=r.retrieve_safe_object("attributi"),
            ),
        )
        file_ids = r.retrieve_int_array("fileIds")
        for file_id in file_ids:
            await db.instance.add_file_to_sample(conn, file_id, sample_id)

    return {"id": sample_id}


@app.patch("/api/samples")
async def update_sample() -> JSONDict:
    r = JSONChecker(await quart_safe_json_dict(), "request")

    async with db.instance.begin() as conn:
        sample_id = r.retrieve_safe_int("id")
        attributi = await db.instance.retrieve_attributi(conn, AssociatedTable.SAMPLE)
        sample = await db.instance.retrieve_sample(conn, sample_id, attributi)
        assert sample is not None
        sample_attributi = sample.attributi
        sample_attributi.extend_with_attributi_map(
            AttributiMap.from_types_and_json(
                attributi,
                sample_ids=[],
                raw_attributi=r.retrieve_safe_object("attributi"),
            )
        )
        await db.instance.update_sample(
            conn,
            id_=sample_id,
            name=r.retrieve_safe_str("name"),
            attributi=sample_attributi,
        )
        await db.instance.remove_files_from_sample(conn, sample_id)
        file_ids = r.retrieve_int_array("fileIds")
        for file_id in file_ids:
            await db.instance.add_file_to_sample(conn, file_id, sample_id)

    return {}


@app.delete("/api/samples")
async def delete_sample() -> JSONDict:
    r = JSONChecker(await quart_safe_json_dict(), "request")

    async with db.instance.begin() as conn:
        await db.instance.delete_sample(
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


def _encode_sample(a: DBSample) -> JSONDict:
    return {
        "id": a.id,
        "name": a.name,
        "attributi": a.attributi.to_json(),
        "files": [_encode_file(f) for f in a.files],
    }


@app.get("/api/samples")
async def read_samples() -> JSONDict:
    async with db.instance.begin() as conn:
        attributi = await db.instance.retrieve_attributi(
            conn, associated_table=AssociatedTable.SAMPLE
        )
        result: JSONDict = {
            "samples": [
                _encode_sample(a)
                for a in await db.instance.retrieve_samples(conn, attributi)
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
                sample_ids=[],
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
        sample_ids = await db.instance.retrieve_sample_ids(conn)
        raw_attributi = r.retrieve_safe_object("attributi")
        current_run.attributi.extend_with_attributi_map(
            AttributiMap.from_types_and_json(attributi, sample_ids, raw_attributi)
        )
        await db.instance.update_run_attributi(
            conn,
            id_=run_id,
            attributi=current_run.attributi,
        )

    return {}


@dataclass
class DataSetSummary:
    numberOfRuns: int
    frames: int
    hit_rate: float | None


def _build_run_summary(matching_runs: list[DBRun]) -> DataSetSummary:
    # ugly, but we cannot filter on a function result in a generator expression
    hit_rates = list(
        filter(
            lambda x: x is not None,
            (run.attributi.select_decimal(ATTRIBUTO_HIT_RATE) for run in matching_runs),
        )
    )
    frames = filter(
        lambda x: x is not None,
        (
            run.attributi.select_int(ATTRIBUTO_TARGET_FRAME_COUNT)
            for run in matching_runs
        ),
    )
    return DataSetSummary(
        len(matching_runs),
        sum(frames),  # type: ignore
        mean(hit_rates) if hit_rates else None,  # type: ignore
    )


@dataclass(frozen=True)
class DarkRun:
    id: int
    started: datetime.datetime


def determine_latest_dark_run(
    runs: list[DBRun], attributi: list[DBAttributo]
) -> DarkRun | None:
    # We might not have a dark run attribute
    if not any(a.name == ATTRIBUTO_ID_DARK_RUN_TYPE for a in attributi):
        return None
    # Assume runs are ordered descending by ID here
    result = next(
        iter(
            r
            for r in runs
            if r.attributi.select_string(ATTRIBUTO_ID_DARK_RUN_TYPE) is not None
        ),
        None,
    )
    if result is None:
        return None
    started = result.attributi.select_datetime(ATTRIBUTO_STARTED)
    if started is None:
        return None
    return DarkRun(
        result.id,
        started,
    )


@app.post("/api/runs/bulk")
async def read_runs_bulk() -> JSONDict:
    r = JSONChecker(await quart_safe_json_dict(), "request")

    async with db.instance.read_only_connection() as conn:
        attributi = await db.instance.retrieve_attributi(conn, associated_table=None)
        samples = await db.instance.retrieve_samples(conn, attributi)
        return {
            "samples": [_encode_sample(s) for s in samples],
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
                await db.instance.retrieve_sample_ids(conn),
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
        samples = await db.instance.retrieve_samples(conn, attributi)
        experiment_types = await db.instance.retrieve_experiment_types(conn)
        data_sets = await db.instance.retrieve_data_sets(
            conn, [s.id for s in samples], attributi
        )
        all_runs = await db.instance.retrieve_runs(conn, attributi)
        all_events = await db.instance.retrieve_events(conn)

        try:
            run_filter = compile_run_filter(request.args.get("filter", ""))
            runs = [
                run
                for run in all_runs
                if run_filter(
                    FilterInput(run=run, sample_names={s.name: s.id for s in samples})
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

        data_set_id_to_grouped: dict[int, DataSetSummary] = {}
        for ds in data_sets:
            matching_runs = [
                r for r in runs if run_matches_dataset(r.attributi, ds.attributi)
            ]
            data_set_id_to_grouped[ds.id] = _build_run_summary(matching_runs)

        latest_dark = determine_latest_dark_run(runs, attributi)

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
                    "data-sets": [
                        ds.id
                        for ds in data_sets
                        if run_matches_dataset(r.attributi, ds.attributi)
                    ],
                }
                for r in runs
            ],
            "latest-dark": {
                "id": latest_dark.id,
                "started": datetime_to_attributo_int(latest_dark.started),
            }
            if latest_dark
            else None,
            "attributi": [_encode_attributo(a) for a in attributi],
            "experiment-types": {a.name: a.attributo_names for a in experiment_types},
            "data-sets": [
                _encode_data_set(a, data_set_id_to_grouped.get(a.id, None))
                for a in data_sets
            ],
            "events": [_encode_event(e) for e in events],
            "samples": [_encode_sample(a) for a in samples],
            "auto-pilot": (await db.instance.retrieve_configuration(conn)).auto_pilot,
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
        if key != AUTO_PILOT:
            raise CustomWebException(
                code=400,
                title=f'Invalid key "{key}"',
                description=f'Couldn\'t find config key {key}, only know "{AUTO_PILOT}"!',
            )
        return {"value": (await db.instance.retrieve_configuration(conn)).auto_pilot}


@app.patch("/api/user-config/<key>/<value>")
async def update_user_configuration_single(key: str, value: str) -> JSONDict:
    async with db.instance.begin() as conn:
        if key != "auto-pilot":
            raise CustomWebException(
                code=400,
                title=f'Invalid key "{key}"',
                description=f'Couldn\'t find config key {key}, only know "{AUTO_PILOT}"!',
            )

        auto_pilot = value == "True"

        new_configuration = replace(
            await db.instance.retrieve_configuration(conn),
            auto_pilot=auto_pilot,
        )
        await db.instance.update_configuration(
            conn,
            new_configuration,
        )
        return {"value": auto_pilot}


@app.post("/api/experiment-types")
async def create_experiment_type() -> JSONDict:
    r = JSONChecker(await quart_safe_json_dict(), "request")

    async with db.instance.begin() as conn:
        await db.instance.create_experiment_type(
            conn,
            name=r.retrieve_safe_str("name"),
            experiment_attributi_names=r.retrieve_string_array("attributi-names"),
        )

    return {}


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
        new_experiment_type = r.optional_str("experiment-type")
        # There is no semantic yet for resetting an experiment type
        if new_experiment_type is None:
            return {}

        experiment_type_instance = next(
            iter(
                x
                for x in await db.instance.retrieve_experiment_types(conn)
                if x.name == new_experiment_type
            ),
            None,
        )
        if experiment_type_instance is None:
            raise CustomWebException(
                code=404,
                title=f'Couldn\'t find experiment type "{new_experiment_type}"',
                description="",
            )
        for a in attributi:
            if (
                a.group == ATTRIBUTO_GROUP_MANUAL
                and a.name not in experiment_type_instance.attributo_names
            ):
                run.attributi.remove_but_keep_type(a.name)
        await db.instance.update_run_attributi(conn, run_id, run.attributi)
        return {}


def _encode_experiment_type(a: DBExperimentType) -> JSONDict:
    return {
        "name": a.name,
        "attributo-names": a.attributo_names,
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
            name=r.retrieve_safe_str("name"),
        )

    return {}


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
        experiment_type = r.retrieve_safe_str("experiment-type")

        experiment_type_resolved = next(
            iter(
                et
                for et in await db.instance.retrieve_experiment_types(conn)
                if et.name == experiment_type
            ),
            None,
        )

        if experiment_type_resolved is None:
            raise CustomWebException(
                code=500,
                title=f"Couldn't find experiment type \"{experiment_type}",
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
            await db.instance.retrieve_sample_ids(conn),
            {
                an: run.attributi.select(an)
                for an in experiment_type_resolved.attributo_names
            },
        )

        await db.instance.create_data_set(conn, experiment_type, attributi_map)

        return {}


@app.post("/api/data-sets")
async def create_data_set() -> JSONDict:
    r = JSONChecker(await quart_safe_json_dict(), "request")

    async with db.instance.begin() as conn:
        attributi = await db.instance.retrieve_attributi(conn, associated_table=None)
        attributi_by_name: dict[str, DBAttributo] = {a.name: a for a in attributi}
        sample_ids = await db.instance.retrieve_sample_ids(conn)
        previous_data_sets = await db.instance.retrieve_data_sets(
            conn, sample_ids, attributi
        )

        experiment_type_name = r.retrieve_safe_str("experiment-type")
        experiment_type = next(
            iter(
                t
                for t in await db.instance.retrieve_experiment_types(conn)
                if t.name == experiment_type_name
            ),
            None,
        )

        if experiment_type is None:
            raise CustomWebException(
                code=500,
                title="Invalid experiment type",
                description=f'Experiment type "{experiment_type_name}" not found',
            )

        data_set_attributi = r.retrieve_safe_object("attributi")
        if any(
            x
            for x in previous_data_sets
            if x.experiment_type == experiment_type_name
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
        for attributo_name in experiment_type.attributo_names:
            if attributo_name in data_set_attributi:
                processed_attributi[attributo_name] = data_set_attributi[attributo_name]
            else:
                attributo = attributi_by_name.get(attributo_name, None)
                assert (
                    attributo is not None
                ), f"attributo {attributo_name} mentioned in experiment type {experiment_type_name} not found"
                if not isinstance(attributo.attributo_type, AttributoTypeBoolean):
                    raise CustomWebException(
                        code=500,
                        title="Invalid input",
                        description=f'Got no value for attributo "{attributo_name}"',
                    )
                processed_attributi[attributo_name] = False

        data_set_id = await db.instance.create_data_set(
            conn,
            experiment_type=experiment_type_name,
            attributi=AttributiMap.from_types_and_json(
                attributi,
                sample_ids,
                processed_attributi,
            ),
        )

    return {"id": data_set_id}


def _encode_data_set(a: DBDataSet, summary: DataSetSummary | None) -> JSONDict:
    result = {
        "id": a.id,
        "experiment-type": a.experiment_type,
        "attributi": a.attributi.to_json(),
    }
    if summary is not None:
        result["summary"] = {
            "number-of-runs": summary.numberOfRuns,
            "frames": summary.frames,
            "hit-rate": summary.hit_rate,
        }
    return result


@app.get("/api/data-sets")
async def read_data_sets() -> JSONDict:
    async with db.instance.read_only_connection() as conn:
        if _has_artificial_delay():
            await asyncio.sleep(3)
        attributi = list(
            await db.instance.retrieve_attributi(conn, associated_table=None)
        )
        samples = await db.instance.retrieve_samples(conn, attributi)
        experiment_types = await db.instance.retrieve_experiment_types(conn)
        return {
            "data-sets": [
                _encode_data_set(a, summary=None)
                for a in await db.instance.retrieve_data_sets(
                    conn,
                    [s.id for s in samples],
                    attributi,
                )
            ],
            "samples": [_encode_sample(s) for s in samples],
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

        if found_attributo.associated_table == AssociatedTable.SAMPLE:
            for s in await db.instance.retrieve_samples(conn, attributi):
                s.attributi.remove_with_type(AttributoId(attributo_name))
                await db.instance.update_sample(conn, s.id, s.name, s.attributi)
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


@app.get("/api/analysis/analysis-results")
async def read_analysis_results() -> JSONDict:
    async with db.instance.read_only_connection() as conn:
        attributi = await db.instance.retrieve_attributi(conn, associated_table=None)
        samples = await db.instance.retrieve_samples(conn, attributi)
        data_sets = await db.instance.retrieve_data_sets(
            conn, [x.id for x in samples], attributi
        )
        data_sets_by_experiment_type: dict[str, list[DBDataSet]] = group_by(
            data_sets,
            lambda ds: ds.experiment_type,
        )
        runs = await db.instance.retrieve_runs(conn, attributi)
        data_set_to_runs: dict[int, list[DBRun]] = {
            ds.id: [r for r in runs if run_matches_dataset(r.attributi, ds.attributi)]
            for ds in data_sets
        }
        analysis_results = await db.instance.retrieve_cfel_analysis_results(conn)
        data_set_to_analysis_results: dict[int, list[DBCFELAnalysisResult]] = group_by(
            analysis_results,
            lambda key: key.data_set_id,
        )

        return {
            "attributi": [_encode_attributo(a) for a in attributi],
            "sample-id-to-name": [[x.id, x.name] for x in samples],
            "experiment-types": {
                experiment_type: [
                    {
                        "data-set": _encode_data_set(
                            ds,
                            _build_run_summary(data_set_to_runs.get(ds.id, [])),
                        ),
                        "analysis-results": [
                            _encode_cfel_analysis_result(k)
                            for k in sorted(
                                data_set_to_analysis_results.get(ds.id, []),
                                key=lambda r: r.data_set_id,
                            )
                        ],
                        "runs": [
                            str(t[0]) if t[0] == t[1] else f"{t[0]}-{t[1]}"
                            for t in create_intervals(
                                [r.id for r in data_set_to_runs.get(ds.id, [])]
                            )
                        ],
                    }
                    for ds in data_sets
                ]
                for experiment_type, data_sets in data_sets_by_experiment_type.items()
            },
        }


def _encode_cfel_analysis_result(k: DBCFELAnalysisResult) -> JSONDict:
    return {
        "id": k.id,
        "directoryName": k.directory_name,
        "dataSetId": k.data_set_id,
        "resolution": k.resolution,
        "rsplit": k.rsplit,
        "cchalf": k.cchalf,
        "ccstar": k.ccstar,
        "snr": k.snr,
        "completeness": k.completeness,
        "multiplicity": k.multiplicity,
        "totalMeasurements": k.total_measurements,
        "uniqueReflections": k.unique_reflections,
        "numPatterns": k.num_patterns,
        "numHits": k.num_hits,
        "indexedPatterns": k.indexed_patterns,
        "indexedCrystals": k.indexed_crystals,
        "crystfelVersion": k.crystfel_version,
        "ccstarRSplit": k.ccstar_rsplit,
        "created": datetime_to_attributo_int(k.created),
        "files": [_encode_file(f) for f in k.files],
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
