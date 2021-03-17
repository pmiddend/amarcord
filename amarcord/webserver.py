import datetime
import json
import logging

from flask import Flask, request
from flask_cors import CORS
from werkzeug.exceptions import HTTPException

from amarcord.config import load_config
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributi import (
    attributo_type_to_schema,
)
from amarcord.db.attributo_id import AttributoId
from amarcord.db.comment import DBComment
from amarcord.db.db import (
    DB,
    DBRun,
)
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.proposal_id import ProposalId
from amarcord.db.sample_data import create_sample_data
from amarcord.db.tables import create_tables
from amarcord.modules.dbcontext import CreationMode, DBContext
from amarcord.modules.json import JSONDict

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)


app = Flask(__name__)
CORS(app)

config = load_config()

dbcontext = DBContext(config["db"]["url"])
tables = create_tables(dbcontext)

dbcontext.create_all(creation_mode=CreationMode.CHECK_FIRST)

if (
    isinstance(config["db"]["create_sample_data"], bool)
    and config["db"]["create_sample_data"]
):
    create_sample_data(dbcontext, tables)

db = DB(
    dbcontext,
    tables,
)


def _convert_run(r: DBRun) -> JSONDict:
    def convert_comment(value: DBComment) -> JSONDict:
        return {
            "id": value.id,
            "text": value.text,
            "author": value.author,
            "created": value.created.isoformat(),
        }

    return {
        "id": r.id,
        "sample_id": r.sample_id,
        "modified": r.modified.isoformat(),
        "comments": [convert_comment(c) for c in r.comments],
        "attributi": r.attributi.to_json(),
    }


@app.route("/<int:proposal_id>/runs")
def retrieve_runs(proposal_id: int) -> JSONDict:
    # pylint: disable=global-statement
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


def _convert_metadata(v: DBAttributo) -> JSONDict:
    return {
        "name": v.name,
        "description": v.description,
        "type_schema": attributo_type_to_schema(v.attributo_type)
        if v.attributo_type is not None
        else None,
    }


@app.route("/run_properties")
def retrieve_run_properties() -> JSONDict:
    # pylint: disable=global-statement
    global db
    with db.connect() as conn:
        return {
            "attributi": [
                _convert_metadata(v)
                for v in db.retrieve_table_attributi(conn, AssociatedTable.RUN).values()
            ]
        }


@app.route("/run/<int:run_id>")
def retrieve_run(run_id: int) -> JSONDict:
    # pylint: disable=global-statement
    global db
    with db.connect() as conn:
        return _convert_run(db.retrieve_run(conn, run_id))


@app.route("/run/<int:run_id>/comment", methods=["POST"])
def add_comment(
    run_id: int,
) -> JSONDict:
    # pylint: disable=global-statement
    global db
    with db.connect() as conn:
        assert isinstance(request.json, dict)
        assert "author" in request.json
        assert "text" in request.json
        db.add_comment(conn, run_id, request.json["author"], request.json["text"])
        return {}


@app.route("/run/<int:run_id>/attributo/<attributo_name>", methods=["POST"])
def update_run_attributo(
    run_id: int,
    attributo_name: str,
) -> JSONDict:
    # pylint: disable=global-statement
    global db
    with db.connect() as conn:
        assert isinstance(request.json, dict)
        assert "value" in request.json
        db.update_run_attributo(
            conn, run_id, AttributoId(attributo_name), request.json["value"]
        )
        return {}


@app.route("/run/<int:run_id>/comment/<int:comment_id>", methods=["DELETE"])
def delete_comment(run_id: int, comment_id: int) -> JSONDict:
    # pylint: disable=global-statement
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
