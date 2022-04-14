import asyncio
import datetime
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Dict, cast, List, Optional, Tuple

from pint import UnitRegistry
from quart import Quart, request, redirect, Response
from quart_cors import cors
from tap import Tap
from werkzeug.exceptions import HTTPException

from amarcord.amici.om.client import (
    ATTRIBUTO_NUMBER_OF_HITS,
    ATTRIBUTO_NUMBER_OF_FRAMES,
)
from amarcord.amici.xfel.karabo_bridge import ATTRIBUTO_ID_DARK_RUN_TYPE
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributi import (
    AttributoConversionFlags,
    datetime_to_attributo_int,
    ATTRIBUTO_STARTED,
    ATTRIBUTO_STOPPED,
)
from amarcord.db.attributi import attributo_type_to_schema
from amarcord.db.attributi import schema_to_attributo_type
from amarcord.db.attributi_map import (
    AttributiMap,
    run_matches_dataset,
    JsonAttributiMap,
)
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoTypeBoolean
from amarcord.db.cfel_analysis_result import DBCFELAnalysisResult
from amarcord.db.data_set import DBDataSet
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.experiment_type import DBExperimentType
from amarcord.db.table_classes import DBFile, DBEvent, DBSample, DBRun
from amarcord.json import JSONDict
from amarcord.json_checker import JSONChecker
from amarcord.json_schema import parse_schema_type
from amarcord.quart_utils import CustomJSONEncoder, CustomWebException
from amarcord.quart_utils import QuartDatabases
from amarcord.quart_utils import handle_exception
from amarcord.quart_utils import quart_safe_json_dict
from amarcord.util import group_by, create_intervals

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
def error_handler_for_exceptions(e):
    return handle_exception(e)


@app.post("/api/events")
async def create_event() -> JSONDict:
    r = JSONChecker(await quart_safe_json_dict(), "request")

    async with db.instance.begin() as conn:
        event_id = await db.instance.create_event(
            conn,
            level=EventLogLevel.INFO,
            source=r.retrieve_safe_str("source"),
            text=r.retrieve_safe_str("text"),
        )
        file_ids = r.retrieve_int_array("fileIds")
        for file_id in file_ids:
            await db.instance.add_file_to_event(conn, file_id, event_id)
        return {"id": event_id}


@app.get("/")
async def index() -> Response:
    return redirect("/index.html")


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
        result = {
            "samples": [
                _encode_sample(a)
                for a in await db.instance.retrieve_samples(conn, attributi)
            ],
            "attributi": [_encode_attributo(a) for a in attributi],
        }
        if _has_artificial_delay():
            await asyncio.sleep(3)
        return result  # type: ignore


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
    hits: int


def build_run_summary(matching_runs: List[DBRun]) -> DataSetSummary:
    result: DataSetSummary = DataSetSummary(
        numberOfRuns=len(matching_runs), frames=0, hits=0
    )
    for run in matching_runs:
        hit_result = run.attributi.select_int(ATTRIBUTO_NUMBER_OF_HITS)
        if hit_result is not None:
            result.hits += hit_result
        frames_result = run.attributi.select_int(ATTRIBUTO_NUMBER_OF_FRAMES)
        if frames_result is not None:
            result.frames += frames_result
    return result


# This function is not really needed, but it's just nicer to have the attribut in the runs table sorted by...
# 0. ID (not an attributo)
# 1. "started time"
# 2. "stopped time"
# _. "the rest"
def attributo_sort_key(r: DBAttributo) -> Tuple[int, str]:
    return (
        0 if r.name == ATTRIBUTO_STARTED else 1 if r.name == ATTRIBUTO_STOPPED else 2,
        r.name,
    )


@dataclass(frozen=True)
class DarkRun:
    id: int
    started: datetime.datetime


def determine_latest_dark_run(
    runs: List[DBRun], attributi: List[DBAttributo]
) -> Optional[DarkRun]:
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


@app.get("/api/runs")
async def read_runs() -> JSONDict:
    async with db.instance.begin() as conn:
        attributi = await db.instance.retrieve_attributi(conn, associated_table=None)
        attributi.sort(key=attributo_sort_key)
        samples = await db.instance.retrieve_samples(conn, attributi)
        experiment_types = await db.instance.retrieve_experiment_types(conn)
        data_sets = await db.instance.retrieve_data_sets(
            conn, [s.id for s in samples], attributi
        )
        runs = await db.instance.retrieve_runs(conn, attributi)
        data_set_id_to_grouped: Dict[int, DataSetSummary] = {}
        for ds in data_sets:
            matching_runs = [
                r for r in runs if run_matches_dataset(r.attributi, ds.attributi)
            ]
            data_set_id_to_grouped[ds.id] = build_run_summary(matching_runs)

        latest_dark = determine_latest_dark_run(runs, attributi)

        result = {
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
            "experiment-types": [a.name for a in experiment_types],
            "data-sets": [
                _encode_data_set(a, data_set_id_to_grouped.get(a.id, None))
                for a in data_sets
            ],
            "events": [
                _encode_event(e) for e in await db.instance.retrieve_events(conn)
            ],
            "samples": [_encode_sample(a) for a in samples],
        }
        if _has_artificial_delay():
            await asyncio.sleep(3)
        return result  # type: ignore


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
            ]
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


@app.post("/api/data-sets")
async def create_data_set() -> JSONDict:
    r = JSONChecker(await quart_safe_json_dict(), "request")

    async with db.instance.begin() as conn:
        attributi = await db.instance.retrieve_attributi(conn, associated_table=None)
        attributi_by_name: Dict[str, DBAttributo] = {a.name: a for a in attributi}
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


def _encode_data_set(a: DBDataSet, summary: Optional[DataSetSummary]) -> JSONDict:
    result = {
        "id": a.id,
        "experiment-type": a.experiment_type,
        "attributi": a.attributi.to_json(),
    }
    if summary is not None:
        result["summary"] = {
            "number-of-runs": summary.numberOfRuns,
            "hits": summary.hits,
            "frames": summary.frames,
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
                await db.instance.update_sample(
                    conn, cast(int, s.id), s.name, s.attributi
                )
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
        "type": attributo_type_to_schema(a.attributo_type),
    }


@app.get("/api/files/<int:file_id>")
async def read_file(file_id: int):
    async with db.instance.read_only_connection() as conn:
        (
            file_name,
            mime_type,
            contents,
            _file_size_in_bytes,
        ) = await db.instance.retrieve_file(conn, file_id)

    async def async_generator():
        yield contents

    headers = {"Content-Type": mime_type}
    # Content-Disposition makes it so the browser opens a "Save file as" dialog. For images, PDFs, ..., we can just
    # display them in the browser instead.
    dont_do_disposition = ["image", "application/pdf", "text/plain"]
    if all(not mime_type.startswith(x) for x in dont_do_disposition):
        headers["Content-Disposition"] = f'attachment; filename="{file_name}"'
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
    async with db.instance.begin() as conn:
        attributi = await db.instance.retrieve_attributi(conn, associated_table=None)
        samples = await db.instance.retrieve_samples(conn, attributi)
        data_sets = await db.instance.retrieve_data_sets(
            conn, [x.id for x in samples], attributi
        )
        data_sets_by_experiment_type: Dict[str, List[DBDataSet]] = group_by(
            data_sets,
            lambda ds: ds.experiment_type,
        )
        runs = await db.instance.retrieve_runs(conn, attributi)
        data_set_to_runs: Dict[int, List[DBRun]] = {
            ds.id: [r for r in runs if run_matches_dataset(r.attributi, ds.attributi)]
            for ds in data_sets
        }
        analysis_results = await db.instance.retrieve_cfel_analysis_results(conn)
        data_set_to_analysis_results: Dict[int, List[DBCFELAnalysisResult]] = group_by(
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
                            build_run_summary(data_set_to_runs.get(ds.id, [])),
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
    debug: bool = True
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
    app.run(host=args.host, port=args.port, debug=False, use_reloader=args.debug)
    return 0


if __name__ == "__main__":
    sys.exit(main())
