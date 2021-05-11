# pylint: disable=global-statement
import json
import logging
import os

import sqlalchemy as sa
from flask import Flask
from flask import request
from flask_cors import CORS
from werkzeug.exceptions import HTTPException
from werkzeug.utils import redirect

from amarcord.amici.p11.db import DiffractionType
from amarcord.amici.p11.db import table_crystals
from amarcord.amici.p11.db import table_dewar_lut
from amarcord.amici.p11.db import table_diffractions
from amarcord.amici.p11.db import table_pucks
from amarcord.modules.dbcontext import Connection
from amarcord.modules.dbcontext import DBContext
from amarcord.modules.json import JSONDict

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)


app = Flask(
    __name__,
    static_folder=os.environ.get(
        "AMARCORD_STATIC_FOLDER",
        os.getcwd()
        + "/purescript-frontend/"
        + ("dist" if "AMARCORD_USE_DIST" in os.environ else "prod"),
    ),
    static_url_path="/",
)
CORS(app)


@app.route("/")
def hello():
    return redirect("/index.html")


def _retrieve_dewar_table(conn: Connection, dewar_lut: sa.Table) -> JSONDict:
    return {
        "dewarTable": [
            {"puckId": row[0], "dewarPosition": row[1]}
            for row in conn.execute(
                sa.select([dewar_lut.c.puck_id, dewar_lut.c.dewar_position]).order_by(
                    dewar_lut.c.dewar_position
                )
            ).fetchall()
        ]
    }


@app.route("/api/pucks")
def retrieve_pucks() -> JSONDict:
    dbcontext = DBContext(os.environ["AMARCORD_DB_URL"])
    pucks = table_pucks(dbcontext.metadata)
    with dbcontext.connect() as conn:
        return {
            "pucks": [
                {"puckId": row[0]}
                for row in conn.execute(sa.select([pucks.c.puck_id])).fetchall()
            ]
        }


def _retrieve_diffractions(
    conn: Connection,
    diffractions: sa.Table,
    crystals: sa.Table,
    puck_id: str,
    only_undiffracted: bool,
) -> JSONDict:
    # noinspection PyComparisonWithNone
    return {
        "diffractions": [
            {
                "crystalId": row[0],
                "runId": row[1],
                "dewarPosition": row[2],
                "diffraction": row[3].value if row[3] is not None else None,
                "comment": row[4],
                "puckPositionId": row[5],
            }
            for row in conn.execute(
                sa.select(
                    [
                        crystals.c.crystal_id,
                        diffractions.c.run_id,
                        diffractions.c.dewar_position,
                        diffractions.c.diffraction,
                        diffractions.c.comment,
                        crystals.c.puck_position_id,
                    ]
                )
                .select_from(crystals.outerjoin(diffractions))
                .where(
                    sa.and_(
                        crystals.c.puck_id == puck_id,
                        True if not only_undiffracted
                        # Not sure if sqlalchemy can take this
                        # pylint: disable=singleton-comparison
                        else diffractions.c.diffraction != None,
                    )
                )
                .order_by(crystals.c.puck_position_id)
            ).fetchall()
        ]
    }


@app.route("/api/diffraction/<puck_id>")
def retrieve_diffractions(puck_id: str) -> JSONDict:
    dbcontext = DBContext(os.environ["AMARCORD_DB_URL"])
    diffractions = table_diffractions(dbcontext.metadata)
    crystals = table_crystals(dbcontext.metadata)
    with dbcontext.connect() as conn:
        return _retrieve_diffractions(conn, diffractions, crystals, puck_id, False)


@app.route("/api/diffraction/<puck_id>", methods=["POST"])
def add_diffraction(puck_id: str) -> JSONDict:
    dbcontext = DBContext(os.environ["AMARCORD_DB_URL"])
    diffractions = table_diffractions(dbcontext.metadata)
    crystals = table_crystals(dbcontext.metadata)
    with dbcontext.connect() as conn:
        conn.execute(
            sa.insert(diffractions).values(
                crystal_id=request.form["crystalId"],
                run_id=int(request.form["runId"]),
                diffraction=DiffractionType(request.form["diffraction"]),
                beam_intensity=request.form["beamIntensity"],
                pinhole=request.form["pinhole"],
                focusing=request.form["focusing"],
                comment=request.form["comment"],
            )
        )
        return _retrieve_diffractions(conn, diffractions, crystals, puck_id, False)


@app.route("/api/dewar/<int:dewarPosition>/<puckId>", methods=["GET"])
def add_puck_to_table(dewarPosition: int, puckId: str) -> JSONDict:
    dbcontext = DBContext(os.environ["AMARCORD_DB_URL"])
    dewar_lut = table_dewar_lut(dbcontext.metadata)
    with dbcontext.connect() as conn:
        conn.execute(
            sa.insert(dewar_lut).values(dewar_position=dewarPosition, puck_id=puckId)
        )
        return _retrieve_dewar_table(conn, dewar_lut)


@app.route("/api/dewar")
def retrieve_dewar_table() -> JSONDict:
    dbcontext = DBContext(os.environ["AMARCORD_DB_URL"])
    dewar_lut = table_dewar_lut(dbcontext.metadata)
    with dbcontext.connect() as conn:
        return _retrieve_dewar_table(conn, dewar_lut)


@app.route("/api/dewar/<int:position>", methods=["DELETE"])
def remove_single_dewar_entry(position: int) -> JSONDict:
    dbcontext = DBContext(os.environ["AMARCORD_DB_URL"])
    dewar_lut = table_dewar_lut(dbcontext.metadata)
    with dbcontext.connect() as conn:
        conn.execute(sa.delete(dewar_lut).where(dewar_lut.c.dewar_position == position))
        return _retrieve_dewar_table(conn, dewar_lut)


@app.route("/api/dewar", methods=["DELETE"])
def remove_dewar_lut() -> JSONDict:
    dbcontext = DBContext(os.environ["AMARCORD_DB_URL"])
    dewar_lut = table_dewar_lut(dbcontext.metadata)
    with dbcontext.connect() as conn:
        conn.execute(sa.delete(dewar_lut))
        return _retrieve_dewar_table(conn, dewar_lut)


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
