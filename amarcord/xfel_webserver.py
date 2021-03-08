import datetime
import json
import logging
import os
from typing import Any, Dict

from flask import Flask, request
from flask_cors import CORS
from werkzeug.exceptions import HTTPException

from amarcord.config import load_config
from amarcord.modules.dbcontext import CreationMode, DBContext
from amarcord.modules.json import JSONDict, JSONValue
from amarcord.modules.spb.db import (
    DB,
    DBCustomProperty,
    DBRunComment,
    RunPropertyValue,
)
from amarcord.modules.properties import property_type_to_schema
from amarcord.modules.spb.db_tables import create_sample_data, create_tables
from amarcord.modules.spb.proposal_id import ProposalId
from amarcord.modules.spb.run_property import RunProperty

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)


app = Flask(__name__)
CORS(app)

config = load_config()

dbcontext = DBContext(config["db"]["url"])
tables = create_tables(dbcontext)

dbcontext.create_all(creation_mode=CreationMode.CHECK_FIRST)

if "AMARCORD_CREATE_SAMPLE_DATA" in os.environ:
    create_sample_data(dbcontext, tables)

db = DB(
    dbcontext,
    tables,
)


def _convert_run(r: Dict[RunProperty, RunPropertyValue]) -> JSONDict:
    def _convert_to_json(value: Any) -> JSONValue:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, datetime.datetime):
            return value.isoformat()
        if isinstance(value, list):
            return [_convert_to_json(av) for av in value]
        if isinstance(value, DBRunComment):
            return {
                "id": value.id,
                "text": value.text,
                "author": value.author,
                "created": value.created.isoformat(),
            }
        raise Exception(f"invalid property type in run: {type(value)}")

    result: JSONDict = {}
    for k, v in r.items():
        result[k] = _convert_to_json(v)
    return result


@app.route("/<int:proposal_id>/runs")
def retrieve_runs(proposal_id: int) -> JSONDict:
    global db
    with db.connect() as conn:
        since = request.args.get("since", None)
        return {
            "runs": [
                _convert_run(r)
                for r in db.retrieve_runs(
                    conn,
                    ProposalId(proposal_id),
                    datetime.datetime.fromisoformat(since)
                    if since is not None
                    else None,
                )
            ]
        }


def _convert_metadata(v: DBCustomProperty) -> JSONDict:
    return {
        "name": v.name,
        "description": v.description,
        "suffix": v.suffix,
        "type_schema": property_type_to_schema(v.rich_property_type)
        if v.rich_property_type is not None
        else None,
    }


@app.route("/run_properties")
def retrieve_run_properties() -> JSONDict:
    global db
    with db.connect() as conn:
        return {
            "metadata": [
                _convert_metadata(v) for v in db.run_property_metadata(conn).values()
            ]
        }


@app.route("/run/<int:run_id>")
def retrieve_run(run_id: int) -> JSONDict:
    global db
    with db.connect() as conn:
        run = db.retrieve_run(conn, run_id)
        run_props = _convert_run(run.properties)
        return {"run": run_props, "manual_properties": list(run.manual_properties)}


@app.route("/run/<int:run_id>/comment", methods=["POST"])
def add_comment(
    run_id: int,
) -> JSONDict:
    global db
    with db.connect() as conn:
        assert isinstance(request.json, dict)
        assert "author" in request.json
        assert "text" in request.json
        db.add_comment(conn, run_id, request.json["author"], request.json["text"])
        return {}


@app.route("/run/<int:run_id>/comment/<int:comment_id>", methods=["DELETE"])
def delete_comment(run_id: int, comment_id: int) -> JSONDict:
    global db
    with db.connect() as conn:
        db.delete_comment(conn, run_id, comment_id)
        return {}


@app.errorhandler(HTTPException)
def handle_exception(e):
    """Return JSON instead of HTML for HTTP errors."""
    # start with the correct headers and status code from the error
    response = e.get_response()
    # replace the body with JSON
    response.data = json.dumps(
        {
            "code": e.code,
            "name": e.name,
            "description": e.description,
        }
    )
    response.content_type = "application/json"
    return response
