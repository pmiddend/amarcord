# pylint: disable=global-statement
import json
import logging
import os
import sys

from flask import Flask
from flask import request
from flask_cors import CORS
from werkzeug.exceptions import HTTPException

from amarcord.config import load_user_config
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributi import (
    attributo_type_to_schema,
)
from amarcord.db.attributo_id import AttributoId
from amarcord.db.db import DB
from amarcord.db.db import OverviewAttributi
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.mini_sample import DBMiniSample
from amarcord.db.proposal_id import ProposalId
from amarcord.db.sample_data import create_sample_data
from amarcord.db.table_classes import DBEvent
from amarcord.db.tables import create_tables
from amarcord.modules.dbcontext import CreationMode
from amarcord.modules.dbcontext import DBContext
from amarcord.modules.json import JSONArray
from amarcord.modules.json import JSONDict

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)


app = Flask(
    __name__,
    static_folder=os.environ.get("AMARCORD_STATIC_FOLDER", "purescript-frontend/prod"),
    static_url_path="/",
)
CORS(app)

db_url: str
is_create_sample_data: bool
if "AMARCORD_DB_URL" in os.environ:
    db_url = os.environ["AMARCORD_DB_URL"]
    csd = os.environ.get("AMARCORD_CREATE_SAMPLE_DATA", "false")
    is_create_sample_data = csd == "true"
else:
    config = load_user_config()
    db_url_ = config.get("db_url", None)
    if db_url_ is None:
        sys.stderr.write('Couldn\'t find "db_url" in configuration!')
        sys.exit(1)
    db_url = db_url_
    is_create_sample_data = bool(config["create_sample_data"])


dbcontext = DBContext(db_url)
tables = create_tables(dbcontext)

if db_url.startswith("sqlite://"):
    dbcontext.create_all(creation_mode=CreationMode.CHECK_FIRST)

db = DB(
    dbcontext,
    tables,
)

if is_create_sample_data:
    create_sample_data(db)


def _convert_overview(r: OverviewAttributi) -> JSONArray:
    return [
        v
        for table, attributi in r.items()
        for v in attributi.to_raw(ignore_comments=False).to_json_array(table.value)
    ]


def _convert_mini_sample(r: DBMiniSample) -> JSONDict:
    return {"id": r.sample_id, "name": r.sample_name}


@app.route("/api/minisamples")
def retrieve_mini_samples() -> JSONDict:
    proposal_id = int(os.environ["AMARCORD_PROPOSAL_ID"])
    global db
    with db.connect() as conn:
        return {
            "samples": [
                _convert_mini_sample(r)
                for r in db.retrieve_mini_samples(conn, ProposalId(proposal_id))
            ]
        }


@app.route("/api/change_run_sample/<int:run_id>/<int:sample_id>")
def change_run_sample(run_id: int, sample_id: int) -> JSONDict:
    _proposal_id = int(os.environ["AMARCORD_PROPOSAL_ID"])
    global db
    with db.connect() as conn:
        db.update_run_attributo(conn, run_id, AttributoId("sample_id"), sample_id)
        return {}


@app.route("/api/overview")
def retrieve_overview() -> JSONDict:
    proposal_id = int(os.environ["AMARCORD_PROPOSAL_ID"])
    global db
    with db.connect() as conn:
        # since = request.args.get("since", None)
        # if since is not None:
        #     since = datetime.datetime.fromisoformat(since)
        return {
            "overviewRows": [
                _convert_overview(r)
                for r in db.retrieve_overview(
                    conn, ProposalId(proposal_id), db.retrieve_attributi(conn)
                )
            ]
        }


def _convert_metadata(table: AssociatedTable, v: DBAttributo) -> JSONDict:
    return {
        "table": table.value,
        "name": v.name,
        "description": v.description,
        "typeSchema": attributo_type_to_schema(v.attributo_type)
        if v.attributo_type is not None
        else None,
    }


def _convert_event(e: DBEvent) -> JSONDict:
    return {
        "id": e.id,
        "source": e.source,
        "created": e.created,
        "level": e.level.value,
        "text": e.text,
    }


@app.route("/api/events")
def retrieve_events() -> JSONDict:
    global db
    with db.connect() as conn:
        return {"events": [_convert_event(a) for a in db.retrieve_events(conn)]}


@app.route("/api/attributi")
def retrieve_attributi() -> JSONDict:
    global db
    with db.connect() as conn:
        return {
            "attributi": [
                _convert_metadata(table, a)
                for table, attributi in db.retrieve_attributi(conn).items()
                for a in attributi.values()
            ]
        }


# @app.route("/run/<int:run_id>")
# def retrieve_run(run_id: int) -> JSONDict:
#     global db
#     with db.connect() as conn:
#         return _convert_run(db.retrieve_run(conn, run_id))


@app.route("/api/run/<int:run_id>/comment", methods=["POST"])
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


@app.route("/api/run/<int:run_id>/comment/<int:comment_id>", methods=["DELETE"])
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
