import asyncio
import json
import os
import sys
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import cast

from pint import UnitRegistry
from quart import Quart, request
from quart_cors import cors
from tap import Tap
from werkzeug.exceptions import HTTPException

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributi import AttributoConversionFlags, datetime_to_attributo_string
from amarcord.db.attributi import attributo_type_to_schema
from amarcord.db.attributi import schema_to_attributo_type
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.attributo_id import AttributoId
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.table_classes import DBFile, DBEvent, DBSample
from amarcord.json import JSONDict
from amarcord.json_checker import JSONChecker
from amarcord.json_schema import parse_schema_type
from amarcord.quart_utils import CustomJSONEncoder
from amarcord.quart_utils import QuartDatabases
from amarcord.quart_utils import handle_exception
from amarcord.quart_utils import quart_safe_json_dict

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
        return {"id": event_id}


@app.post("/api/samples")
async def create_sample() -> JSONDict:
    r = JSONChecker(await quart_safe_json_dict(), "request")

    async with db.instance.begin() as conn:
        sample_id = await db.instance.create_sample(
            conn,
            name=r.retrieve_safe_str("name"),
            attributi=AttributiMap.from_types_and_json(
                await db.instance.retrieve_attributi(conn, AssociatedTable.SAMPLE),
                r.retrieve_safe_object("attributi"),
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
        await db.instance.update_sample(
            conn,
            id_=sample_id,
            name=r.retrieve_safe_str("name"),
            attributi=AttributiMap.from_types_and_json(
                await db.instance.retrieve_attributi(conn, AssociatedTable.SAMPLE),
                r.retrieve_safe_object("attributi"),
            ),
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
            conn,
            id_=r.retrieve_safe_int("id"),
        )

    return {}


def _encode_file(f: DBFile) -> JSONDict:
    return {
        "id": f.id,
        "description": f.description,
        "type_": f.type_,
        "fileName": f.file_name,
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
        "created": datetime_to_attributo_string(e.created),
        "level": e.level.value,
    }


def _has_artificial_delay() -> bool:
    return bool(app.config.get("ARTIFICIAL_DELAY", False))


@app.get("/api/runs")
async def read_runs() -> JSONDict:
    async with db.instance.begin() as conn:
        attributi = await db.instance.retrieve_attributi(conn, associated_table=None)
        result = {
            "runs": [
                {
                    "id": a.id,
                    "attributi": a.attributi.to_json(),
                    "files": [_encode_file(f) for f in a.files],
                }
                for a in await db.instance.retrieve_runs(conn, attributi)
            ],
            "attributi": [_encode_attributo(a) for a in attributi],
            "events": [
                _encode_event(e) for e in await db.instance.retrieve_events(conn)
            ],
            "samples": [
                _encode_sample(a)
                for a in await db.instance.retrieve_samples(conn, attributi)
            ],
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
                contents_location=Path(temp_file.name),
            )

    return {
        "id": create_result.id,
        "fileName": file_name,
        "description": description,
        "type_": create_result.type_,
    }


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
async def change_attributo() -> JSONDict:
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
            name=r.retrieve_safe_str("nameBefore"),
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
            name=attributo_name,
        )

        if found_attributo.associated_table == AssociatedTable.SAMPLE:
            for s in await db.instance.retrieve_samples(conn, attributi):
                s.attributi.remove(attributo_name)
                await db.instance.update_sample(
                    conn, cast(int, s.id), s.name, s.attributi
                )
        else:
            # FIXME: do this for runs
            pass

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
    async with db.instance.connect() as conn:
        file_name, mime_type, contents = await db.instance.retrieve_file(conn, file_id)

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
    async with db.instance.connect() as conn:
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


class Arguments(Tap):
    port: int = 5000
    host: str = "0.0.0.0"
    db_connection_url: str = "sqlite+aiosqlite://"
    db_echo: bool = False
    debug: bool = True
    delay: bool = False


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
        },
    )
    app.run(host=args.host, port=args.port, debug=args.debug, use_reloader=args.debug)
    return 0


if __name__ == "__main__":
    sys.exit(main())
