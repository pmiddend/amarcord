# pylint: disable=global-statement
import json
import logging
import os
from typing import Any

import sqlalchemy as sa
from flask import Flask
from flask import request
from flask_cors import CORS
from werkzeug.exceptions import BadRequest
from werkzeug.exceptions import HTTPException
from werkzeug.utils import redirect

from amarcord.amici.p11.db import Beamline
from amarcord.amici.p11.db import DiffractionType
from amarcord.amici.p11.db import PuckType
from amarcord.amici.p11.db import ReductionMethod
from amarcord.amici.p11.db import table_crystals
from amarcord.amici.p11.db import table_data_reduction
from amarcord.amici.p11.db import table_dewar_lut
from amarcord.amici.p11.db import table_diffractions
from amarcord.amici.p11.db import table_pucks
from amarcord.modules.dbcontext import Connection
from amarcord.modules.dbcontext import DBContext
from amarcord.modules.json import JSONDict
from amarcord.modules.json import JSONValue

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)


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


def _retrieve_pucks(conn: Connection, pucks: sa.Table) -> JSONDict:
    return {
        "pucks": [
            {"puckId": row[0], "puckType": row[1].value}
            for row in conn.execute(
                sa.select([pucks.c.puck_id, pucks.c.puck_type])
            ).fetchall()
        ]
    }


@app.route("/api/pucks")
def retrieve_pucks() -> JSONDict:
    dbcontext = DBContext(os.environ["AMARCORD_DB_URL"])
    with dbcontext.connect() as conn:
        return _retrieve_pucks(conn, table_pucks(dbcontext.metadata))


def _retrieve_diffractions(
    conn: Connection,
    diffractions: sa.Table,
    crystals: sa.Table,
    puck_id: str,
) -> JSONDict:
    c1 = crystals.alias()
    c2 = crystals.alias()
    # Why this rather complicated SQL statement? The thing is, for each puck ID and position, we have multiple crystals.
    # However, for the beamline GUI, we are only interested in the latest (via date) crystal for each position.
    # And since SQL is a bit iffy w.r.t. selecting the latest value from a group, we have this rather complicated
    # statement. It selects the latest crystal IDs (for each crystal) and returns those, so we can use them in the
    # diffractions table.
    # solution comes from here:
    # https://stackoverflow.com/questions/1313120/retrieving-the-last-record-in-each-group-mysql
    # noinspection PyComparisonWithNone
    latest_crystals = (
        sa.select([c1.c.crystal_id]).select_from(
            c1.outerjoin(
                c2,
                sa.and_(
                    c1.c.puck_id == c2.c.puck_id,
                    c1.c.puck_position_id == c2.c.puck_position_id,
                    c1.c.created < c2.c.created,
                ),
            )
        )
        # pylint: disable=singleton-comparison
        .where(sa.and_(c1.c.puck_id == puck_id, c2.c.created == None))
    ).alias()
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
                .where(crystals.c.crystal_id.in_(latest_crystals))
                .order_by(
                    crystals.c.puck_position_id.asc(), diffractions.c.run_id.asc()
                )
            ).fetchall()
        ]
    }


@app.route("/api/diffraction/<puck_id>")
def retrieve_diffractions(puck_id: str) -> JSONDict:
    dbcontext = DBContext(os.environ["AMARCORD_DB_URL"])
    crystals = table_crystals(dbcontext.metadata, table_pucks(dbcontext.metadata))
    diffractions = table_diffractions(dbcontext.metadata, crystals)
    with dbcontext.connect() as conn:
        return _retrieve_diffractions(conn, diffractions, crystals, puck_id)


@app.post("/api/diffraction/<puck_id>")
def add_diffraction(puck_id: str) -> JSONDict:
    dbcontext = DBContext(os.environ["AMARCORD_DB_URL"])
    crystals = table_crystals(dbcontext.metadata, table_pucks(dbcontext.metadata))
    diffractions = table_diffractions(dbcontext.metadata, crystals)
    with dbcontext.connect() as conn:
        conn.execute(
            sa.insert(diffractions).values(
                crystal_id=request.form["crystalId"],
                run_id=int(request.form["runId"]),
                diffraction=DiffractionType(request.form["diffraction"]),
                beam_intensity=request.form["beamIntensity"],
                comment=request.form["comment"],
            )
        )
        return _retrieve_diffractions(conn, diffractions, crystals, puck_id)


@app.get("/api/dewar/<int:dewarPosition>/<puckId>")
def add_puck_to_table(dewarPosition: int, puckId: str) -> JSONDict:
    dbcontext = DBContext(os.environ["AMARCORD_DB_URL"])
    dewar_lut = table_dewar_lut(dbcontext.metadata, table_pucks(dbcontext.metadata))
    with dbcontext.connect() as conn:
        conn.execute(
            sa.insert(dewar_lut).values(dewar_position=dewarPosition, puck_id=puckId)
        )
        return _retrieve_dewar_table(conn, dewar_lut)


@app.route("/api/dewar")
def retrieve_dewar_table() -> JSONDict:
    dbcontext = DBContext(os.environ["AMARCORD_DB_URL"])
    dewar_lut = table_dewar_lut(dbcontext.metadata, table_pucks(dbcontext.metadata))
    with dbcontext.connect() as conn:
        return _retrieve_dewar_table(conn, dewar_lut)


@app.delete("/api/pucks/<puck_id>")
def remove_puck(puck_id: str) -> JSONDict:
    dbcontext = DBContext(os.environ["AMARCORD_DB_URL"])
    pucks = table_pucks(dbcontext.metadata)
    crystals = table_crystals(dbcontext.metadata, pucks)
    with dbcontext.connect() as conn:
        conn.execute(sa.delete(pucks).where(pucks.c.puck_id == puck_id))
        return _retrieve_sample(conn, pucks, crystals)


@app.delete("/api/crystals/<crystal_id>")
def remove_crystal(crystal_id: str) -> JSONDict:
    dbcontext = DBContext(os.environ["AMARCORD_DB_URL"])
    pucks = table_pucks(dbcontext.metadata)
    crystals = table_crystals(dbcontext.metadata, pucks)
    with dbcontext.connect() as conn:
        conn.execute(sa.delete(crystals).where(crystals.c.crystal_id == crystal_id))
        return _retrieve_sample(conn, pucks, crystals)


@app.delete("/api/dewar/<int:position>")
def remove_single_dewar_entry(position: int) -> JSONDict:
    dbcontext = DBContext(os.environ["AMARCORD_DB_URL"])
    dewar_lut = table_dewar_lut(dbcontext.metadata, table_pucks(dbcontext.metadata))
    with dbcontext.connect() as conn:
        conn.execute(sa.delete(dewar_lut).where(dewar_lut.c.dewar_position == position))
        return _retrieve_dewar_table(conn, dewar_lut)


@app.delete("/api/dewar")
def remove_dewar_lut() -> JSONDict:
    dbcontext = DBContext(os.environ["AMARCORD_DB_URL"])
    dewar_lut = table_dewar_lut(dbcontext.metadata, table_pucks(dbcontext.metadata))
    with dbcontext.connect() as conn:
        conn.execute(sa.delete(dewar_lut))
        return _retrieve_dewar_table(conn, dewar_lut)


def sort_column_to_real_column(
    crystals: sa.Table,
    data_reductions: sa.Table,
    diffractions: sa.Table,
    sort_column: str,
) -> sa.Column:
    if sort_column == "crystalId":
        return crystals.c.crystal_id
    if sort_column == "analysisTime":
        return data_reductions.c.analysis_time
    if sort_column == "drid":
        return data_reductions.c.data_reduction_id
    if sort_column == "resCC":
        return data_reductions.c.resolution_cc
    if sort_column == "angle_step":
        return diffractions.c.angle_step
    if sort_column == "frames":
        return diffractions.c.number_of_frames
    if sort_column == "exposure_time":
        return diffractions.c.exposure_time
    if sort_column == "xray_energy":
        return diffractions.c.xray_energy
    if sort_column == "xray_wavelength":
        return diffractions.c.xray_wavelength
    if sort_column == "detector_distance":
        return diffractions.c.detector_distance
    if sort_column == "aperture_radius":
        return diffractions.c.aperture_radius
    if sort_column == "filter_transmission":
        return diffractions.c.filter_transmission
    if sort_column == "ring_current":
        return diffractions.c.ring_current
    if sort_column == "aperture_horizontal":
        return diffractions.c.aperture_horizontal
    if sort_column == "aperture_vertical":
        return diffractions.c.aperture_vertical
    if sort_column == "resI":
        return data_reductions.c.resolution_isigma
    if sort_column == "isigi":
        return data_reductions.c.isigi
    if sort_column == "rmeas":
        return data_reductions.c.rmeas
    if sort_column == "rfactor":
        return data_reductions.c.rfactor
    if sort_column == "wilsonb":
        return data_reductions.c.Wilson_b
    if sort_column == "cchalf":
        return data_reductions.c.cchalf
    raise BadRequest(f'invalid sort column "{sort_column}"')


def sort_order_to_descending(so: str) -> bool:
    if so == "asc":
        return False
    if so == "desc":
        return True
    raise BadRequest(f'invalid sort order "{so}"')


@app.post("/api/crystals")
def add_crystal() -> JSONDict:
    dbcontext = DBContext(os.environ["AMARCORD_DB_URL"])
    with dbcontext.connect() as conn:
        pucks = table_pucks(dbcontext.metadata)
        crystals = table_crystals(dbcontext.metadata, pucks)
        has_puck_id = request.form["puckId"] != ""
        conn.execute(
            sa.insert(crystals).values(
                crystal_id=request.form["crystalId"],
                puck_id=request.form["puckId"] if has_puck_id else None,
                puck_position_id=int(request.form["puckPosition"])
                if has_puck_id
                else None,
            )
        )
        return _retrieve_sample(conn, pucks, crystals)


@app.post("/api/pucks")
def add_puck() -> JSONDict:
    dbcontext = DBContext(os.environ["AMARCORD_DB_URL"])
    with dbcontext.connect() as conn:
        pucks = table_pucks(dbcontext.metadata)
        conn.execute(
            sa.insert(pucks).values(
                puck_id=request.form["puckId"], puck_type=PuckType.UNI, owner="gui"
            )
        )
        return _retrieve_pucks(conn, pucks)


def _retrieve_sample(conn: Connection, pucks: sa.Table, crystals: sa.Table) -> JSONDict:
    return {
        "pucks": [
            {"puckId": row[0], "puckType": row[1].value}
            for row in conn.execute(
                sa.select([pucks.c.puck_id, pucks.c.puck_type]).order_by(
                    pucks.c.puck_id
                )
            ).fetchall()
        ],
        "crystals": [
            {"crystalId": row[0], "puckId": row[1], "puckPosition": row[2]}
            for row in conn.execute(
                sa.select(
                    [
                        crystals.c.crystal_id,
                        crystals.c.puck_id,
                        crystals.c.puck_position_id,
                    ]
                ).order_by(
                    crystals.c.puck_id,
                    crystals.c.puck_position_id,
                    crystals.c.crystal_id,
                )
            ).fetchall()
        ],
    }


@app.route("/api/sample")
def retrieve_sample() -> JSONDict:
    dbcontext = DBContext(os.environ["AMARCORD_DB_URL"])
    with dbcontext.connect() as conn:
        pucks = table_pucks(dbcontext.metadata)
        crystals = table_crystals(dbcontext.metadata, pucks, schema=None)
        return _retrieve_sample(conn, pucks, crystals)


@app.route("/api/analysis")
def retrieve_analysis() -> JSONDict:
    dbcontext = DBContext(os.environ["AMARCORD_DB_URL"])
    with dbcontext.connect() as conn:
        crystals = table_crystals(
            dbcontext.metadata, table_pucks(dbcontext.metadata), schema=None
        )
        diffractions = table_diffractions(dbcontext.metadata, crystals, schema=None)
        data_reductions = table_data_reduction(
            dbcontext.metadata, crystals, schema=None
        )
        sort_column = request.args.get("sortColumn", "crystal_id")
        sort_order_desc = sort_order_to_descending(request.args.get("sortOrder", "asc"))
        filter_query = request.args.get("filterQuery", "")
        crystal_columns = [
            crystals.c.crystal_id.label("crystals_crystal_id"),
            crystals.c.created.label("crystals_created"),
        ]
        diffraction_columns = [c.label("diff_" + c.name) for c in diffractions.c]
        reduction_columns = [c.label("dr_" + c.name) for c in data_reductions.c]
        all_columns = crystal_columns + diffraction_columns + reduction_columns
        try:
            results = conn.execute(
                sa.select(all_columns)
                .select_from(
                    crystals.outerjoin(diffractions).outerjoin(
                        data_reductions,
                        onclause=sa.and_(
                            data_reductions.c.run_id == diffractions.c.run_id,
                            diffractions.c.crystal_id == data_reductions.c.crystal_id,
                        ),
                    )
                )
                .where(sa.text(filter_query))
                .order_by(sa.desc(sort_column) if sort_order_desc else sort_column)
            ).fetchall()

            def postprocess(v: Any) -> JSONValue:
                if isinstance(v, Beamline):
                    return v.value
                if isinstance(v, DiffractionType):
                    return v.value
                if isinstance(v, ReductionMethod):
                    return v.value
                return v

            return {
                "analysisColumns": [c.name for c in all_columns],
                "analysis": [
                    [postprocess(value) for _, value in row.items()] for row in results
                ],
                "sqlError": None,
            }
        except Exception as e:
            logger.exception(e)
            return {
                "analysisColumns": [c.name for c in all_columns],
                "analysis": [],
                "sqlError": str(e),
            }


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
