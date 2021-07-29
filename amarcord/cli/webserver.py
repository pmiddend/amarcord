import json
import logging
import os
import time
from functools import partial
from pathlib import Path
from threading import Thread
from typing import Callable
from typing import Optional

from flask import Flask
from flask import Response
from flask import current_app
from flask import g
from flask import request
from flask_cors import CORS
from werkzeug.exceptions import BadRequest
from werkzeug.exceptions import HTTPException
from werkzeug.utils import redirect

from amarcord.locker import Locker
from amarcord.modules.dbcontext import Connection
from amarcord.modules.dbcontext import CreationMode
from amarcord.modules.dbcontext import DBContext
from amarcord.modules.json import JSONArray
from amarcord.modules.json import JSONDict
from amarcord.newdb.alembic import upgrade_to_head
from amarcord.newdb.db_crystal import DBCrystal
from amarcord.newdb.db_dewar_lut import DBDewarLUT
from amarcord.newdb.db_diffraction import DBDiffraction
from amarcord.newdb.db_puck import DBPuck
from amarcord.newdb.db_reduction_job import DBJobWithInputsAndOutputs
from amarcord.newdb.db_reduction_job import DBMiniDiffraction
from amarcord.newdb.db_reduction_job import DBMiniReduction
from amarcord.newdb.db_tool import DBTool
from amarcord.newdb.diffraction_type import DiffractionType
from amarcord.newdb.newdb import NewDB
from amarcord.newdb.puck_type import PuckType
from amarcord.newdb.tables import DBTables
from amarcord.workflows.command_line import CommandLine
from amarcord.workflows.job_controller_factory import LocalJobControllerConfig
from amarcord.workflows.job_controller_factory import create_job_controller
from amarcord.workflows.job_controller_factory import parse_job_controller
from amarcord.workflows.workflow_synchronize import bulk_start_jobs
from amarcord.workflows.workflow_synchronize import check_jobs

AMARCORD_DB_URL = "AMARCORD_DB_URL"
AMARCORD_JOB_CONTROLLER = "AMARCORD_JOB_CONTROLLER"

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)


def _create_test_db(db: NewDB, test_files_dir: Path) -> None:
    with db.connect() as conn:
        if db.has_crystals(conn):
            return
        logger.info("First DB access, creating in-memory mock")
        for crystal in ["PLpr_7309_0", "PLpr_7309_1"]:
            db.insert_crystal(conn, DBCrystal(crystal))
            db.insert_diffraction(
                conn,
                DBDiffraction(
                    crystal_id=crystal,
                    run_id=1,
                    diffraction=DiffractionType.success,
                    data_raw_filename_pattern=Path(
                        f"{test_files_dir}/{crystal}/{crystal}_001/*.cbf"
                    ),
                ),
            )
        reduction_tool_id = db.insert_tool(
            conn,
            DBTool(
                id=None,
                name="xia2",
                executable_path=Path().absolute()
                / "workflow-data"
                / "local-dummy-workflow.sh",
                extra_files=[
                    Path().absolute() / "workflow-data" / "dummy-amarcord-output.json"
                ],
                command_line="${diffraction.path} ${testarg}",
                description="description",
                inputs=None,
            ),
        )
        logger.info("added reduction tool with ID %s", reduction_tool_id)
        refinement_tool_id = db.insert_tool(
            conn,
            DBTool(
                id=None,
                name="dmpl",
                executable_path=Path().absolute()
                / "workflow-data"
                / "local-dummy-refinement.sh",
                extra_files=[
                    Path().absolute()
                    / "workflow-data"
                    / "dummy-amarcord-refinement-output.json"
                ],
                command_line="${reduction.mtz_path} ${testarg}",
                description="description2",
                inputs=None,
            ),
        )
        logger.info("added refinement tool with ID %s", refinement_tool_id)


def _create_db(db_url: str) -> NewDB:
    if "AMARCORD_DO_MIGRATIONS" in os.environ:
        upgrade_to_head(db_url)
    dbcontext = DBContext(db_url)
    tables = DBTables(
        dbcontext.metadata,
        with_tools="AMARCORD_WITHOUT_TOOLS" not in os.environ,
        with_estimated_resolution="AMARCORD_WITHOUT_ESTIMATED_RESOLUTION"
        not in os.environ,
        analysis_schema=os.environ.get("AMARCORD_ANALYSIS_SCHEMA", None),
        normal_schema=os.environ.get("AMARCORD_NORMAL_SCHEMA", None),
    )
    if db_url.startswith("sqlite://"):
        dbcontext.create_all(CreationMode.CHECK_FIRST)
    db = NewDB(dbcontext, tables)
    test_files_dir = os.environ.get("AMARCORD_TEST_FILES_DIR")
    if db_url.startswith("sqlite://") and test_files_dir is not None:
        if not Path(test_files_dir).is_dir():
            raise Exception(
                f"AMARCORD_BIG_FILES_DIR {test_files_dir} doesn't exist or is not a directory"
            )
        _create_test_db(db, Path(test_files_dir))
    return db


def workflow_daemon_thread(
    job_controller_str: Optional[str],
    db_url: Optional[str],
    exiter: Optional[Callable[[], bool]] = None,
) -> None:
    if job_controller_str is None or db_url is None:
        return
    controller_config = parse_job_controller(job_controller_str)
    if not isinstance(controller_config, LocalJobControllerConfig):
        return
    job_controller = create_job_controller(controller_config)
    while exiter is None or not exiter():
        time.sleep(5.0)
        with Locker():
            last_check_file = Path("/tmp/last-check")
            if last_check_file.exists():
                last_check = float(last_check_file.read_text().strip())
                if last_check >= time.time() - 5.0:
                    continue
            db = _create_db(db_url)
            with db.connect() as conn:
                check_jobs(job_controller, conn, db)
            last_check_file.write_text(str(time.time()))


workflow_daemon = Thread(
    name="workflow-daemon",
    target=partial(
        workflow_daemon_thread,
        os.environ.get(AMARCORD_JOB_CONTROLLER, None),
        os.environ.get(AMARCORD_DB_URL, None),
    ),
)
workflow_daemon.setDaemon(True)
workflow_daemon.start()


def get_db() -> NewDB:
    if "db" not in g:
        # pylint: disable=assigning-non-slot
        g.db = _create_db(current_app.config["AMARCORD_DB_URL"])

    # pylint: disable=useless-return
    return g.db


# pylint: disable=useless-return
def close_db(_e: Optional[BaseException] = None) -> Response:
    g.pop("db", None)

    # according to the docs, the return values are ignored anyway
    return None  # type: ignore


def create_app() -> Flask:
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

    app.teardown_appcontext(close_db)

    def _merge_env_var_with_config(app_: Flask, env_var: str) -> None:
        var = os.environ.get(env_var)
        if var is None:
            raise Exception(f"couldn't find environment variable {env_var}!")
        app_.config[env_var] = var

    _merge_env_var_with_config(app, AMARCORD_DB_URL)

    @app.route("/")
    def hello():
        return redirect("/index.html")

    def _retrieve_dewar_table(conn: Connection, db: NewDB) -> JSONDict:
        return {
            "dewarTable": [
                {"puckId": row.puck_id, "dewarPosition": row.dewar_position}
                for row in db.retrieve_dewar_table(conn)
            ]
        }

    def _retrieve_pucks(conn: Connection, db: NewDB) -> JSONDict:
        return {
            "pucks": [
                {"puckId": row.id, "puckType": row.puck_type.value}
                for row in db.retrieve_pucks(conn)
            ]
        }

    def _convert_command_line(c: CommandLine) -> JSONArray:
        return [{"name": x.name, "type": x.type_.value} for x in c.inputs]

    def _convert_tool(row: DBTool) -> JSONDict:
        return {
            "toolId": row.id,
            "created": row.created,
            "name": row.name,
            "executablePath": str(row.executable_path),
            "extraFiles": [str(s) for s in row.extra_files],
            "commandLine": row.command_line,
            "description": row.description,
            "inputs": _convert_command_line(row.inputs)
            if row.inputs is not None
            else None,
        }

    def _retrieve_tools(conn: Connection, db: NewDB) -> JSONDict:
        return {"tools": [_convert_tool(row) for row in db.retrieve_tools(conn)]}

    @app.route("/api/workflows/tools")
    def retrieve_tools() -> JSONDict:
        db = get_db()
        with db.connect() as conn:
            return _retrieve_tools(conn, db)

    @app.route("/api/pucks")
    def retrieve_pucks() -> JSONDict:
        db = get_db()
        with db.connect() as conn:
            return _retrieve_pucks(conn, db)

    def _retrieve_diffractions(
        conn: Connection,
        db: NewDB,
        puck_id: str,
    ) -> JSONDict:
        return {
            "diffractions": [
                {
                    "crystalId": row.crystal_id,
                    "runId": row.run_id,
                    "dewarPosition": row.dewar_position,
                    "diffraction": row.diffraction.value
                    if row.diffraction is not None
                    else None,
                    "comment": row.comment,
                    "puckPositionId": row.puck_position_id,
                }
                for row in db.retrieve_beamline_diffractions(conn, puck_id)
            ]
        }

    @app.route("/api/diffraction/<puck_id>")
    def retrieve_diffractions(puck_id: str) -> JSONDict:
        db = get_db()
        with db.connect() as conn:
            return _retrieve_diffractions(conn, db, puck_id)

    @app.post("/api/diffraction/<puck_id>")
    def add_diffraction(puck_id: str) -> JSONDict:
        db = get_db()
        with db.connect() as conn:
            db.insert_diffraction(
                conn,
                DBDiffraction(
                    crystal_id=request.form["crystalId"],
                    run_id=int(request.form["runId"]),
                    diffraction=DiffractionType(request.form["diffraction"]),
                    comment=request.form["comment"],
                    beam_intensity=request.form["beamIntensity"],
                ),
            )
            return _retrieve_diffractions(conn, db, puck_id)

    @app.get("/api/dewar/<int:dewar_position>/<puck_id>")
    def add_puck_to_table(dewar_position: int, puck_id: str) -> JSONDict:
        db = get_db()
        with db.connect() as conn:
            db.insert_dewar_table_entry(conn, DBDewarLUT(dewar_position, puck_id))
            return _retrieve_dewar_table(conn, db)

    @app.route("/api/dewar")
    def retrieve_dewar_table() -> JSONDict:
        db = get_db()
        with db.connect() as conn:
            return _retrieve_dewar_table(conn, db)

    @app.delete("/api/workflows/tools/<int:tool_id>")
    def remove_tool(tool_id: int) -> JSONDict:
        db = get_db()
        with db.connect() as conn:
            db.remove_tool(conn, tool_id)
            return _retrieve_tools(conn, db)

    @app.delete("/api/pucks/<puck_id>")
    def remove_puck(puck_id: str) -> JSONDict:
        db = get_db()
        with db.connect() as conn:
            db.remove_puck(conn, puck_id)
            return _retrieve_sample(conn, db)

    @app.delete("/api/crystals/<crystal_id>")
    def remove_crystal(crystal_id: str) -> JSONDict:
        db = get_db()
        with db.connect() as conn:
            db.remove_crystal(conn, crystal_id)
            return _retrieve_sample(conn, db)

    @app.delete("/api/dewar/<int:position>")
    def remove_single_dewar_entry(position: int) -> JSONDict:
        db = get_db()
        with db.connect() as conn:
            db.remove_dewar_table_entry(conn, position)
            return _retrieve_dewar_table(conn, db)

    @app.delete("/api/dewar")
    def remove_dewar_lut() -> JSONDict:
        db = get_db()
        with db.connect() as conn:
            db.truncate_dewar_table(conn)
            return _retrieve_dewar_table(conn, db)

    def sort_order_to_descending(so: str) -> bool:
        if so == "asc":
            return False
        if so == "desc":
            return True
        raise BadRequest(f'invalid sort order "{so}"')

    @app.get("/api/workflows/jobs")
    def list_jobs() -> JSONDict:
        def convert_job(j: DBJobWithInputsAndOutputs) -> JSONDict:
            return {
                "jobId": j.job.id,
                "started": j.job.started,
                "queued": j.job.queued,
                "stopped": j.job.stopped,
                "status": j.job.status.value,
                "failureReason": j.job.failure_reason,
                "metadata": j.job.metadata,
                "outputDir": str(j.job.output_directory),
                "diffraction": {
                    "runId": j.io.run_id,
                    "crystalId": j.io.crystal_id,
                }
                if isinstance(j.io, DBMiniDiffraction)
                else None,
                "reduction": {
                    "dataReductionId": j.io.data_reduction_id,
                    "mtzPath": str(j.io.mtz_path),
                }
                if isinstance(j.io, DBMiniReduction)
                else None,
                "tool": j.tool.name,
                "toolInputs": j.job.tool_inputs,
            }

        db = get_db()
        with db.connect() as conn:
            return {
                "jobs": [convert_job(j) for j in db.retrieve_jobs_with_attached(conn)]
            }

    @app.post("/api/workflows/jobs/<int:tool_id>")
    def start_job(tool_id: int) -> JSONDict:
        json_content = request.get_json(force=True)
        assert isinstance(
            json_content, dict
        ), f"expected a dictionary for the tool input, got {json_content}"
        inputs = json_content.get("inputs", None)
        assert (
            inputs is not None
        ), f'expected "inputs" in tool input, got {json_content}'
        filter_query = json_content.get("filterQuery", None)
        assert (
            filter_query is not None
        ), f'expected "filter_query" in tool input, got {json_content}'
        limit = json_content.get("limit", None)
        assert limit is None or isinstance(limit, int)
        db = get_db()

        with db.connect() as conn:
            with conn.begin():
                return {
                    "jobIds": bulk_start_jobs(
                        conn, db, tool_id, filter_query, limit, inputs
                    )
                }

    @app.post("/api/workflows/tools")
    def add_tool() -> JSONDict:
        db = get_db()
        with db.connect() as conn:
            db.insert_tool(
                conn,
                DBTool(
                    id=None,
                    name=request.form["name"],
                    executable_path=Path(request.form["executablePath"]),
                    extra_files=[
                        Path(p) for p in json.loads(request.form["extraFiles"])
                    ],
                    command_line=request.form["commandLine"],
                    description=request.form["description"],
                ),
            )
            return _retrieve_tools(conn, db)

    @app.post("/api/workflows/tools/<int:tool_id>")
    def update_tool(tool_id: int) -> JSONDict:
        db = get_db()
        with db.connect() as conn:
            db.update_tool(
                conn,
                DBTool(
                    id=tool_id,
                    name=request.form["name"],
                    executable_path=Path(request.form["executablePath"]),
                    extra_files=[
                        Path(p) for p in json.loads(request.form["extraFiles"])
                    ],
                    command_line=request.form["commandLine"],
                    description=request.form["description"],
                ),
            )
            return _retrieve_tools(conn, db)

    @app.post("/api/crystals")
    def add_crystal() -> JSONDict:
        db = get_db()
        with db.connect() as conn:
            has_puck_id = request.form["puckId"] != ""
            db.insert_crystal(
                conn,
                DBCrystal(
                    crystal_id=request.form["crystalId"],
                    puck_id=request.form["puckId"] if has_puck_id else None,
                    puck_position=int(request.form["puckPosition"])
                    if has_puck_id
                    else None,
                ),
            )
            return _retrieve_sample(conn, db)

    @app.post("/api/pucks")
    def add_puck() -> JSONDict:
        db = get_db()
        with db.connect() as conn:
            db.insert_puck(
                conn,
                DBPuck(id=request.form["puckId"], puck_type=PuckType.UNI, owner="gui"),
            )
            return _retrieve_pucks(conn, db)

    def _retrieve_sample(conn: Connection, db: NewDB) -> JSONDict:
        sample_data = db.retrieve_sample_data(conn)
        return {
            "pucks": [
                {"puckId": row.id, "puckType": row.puck_type.value}
                for row in sample_data.pucks
            ],
            "crystals": [
                {
                    "crystalId": row.crystal_id,
                    "puckId": row.puck_id,
                    "puckPosition": row.puck_position,
                }
                for row in sample_data.crystals
            ],
        }

    @app.route("/api/sample")
    def retrieve_sample() -> JSONDict:
        db = get_db()
        with db.connect() as conn:
            return _retrieve_sample(conn, db)

    @app.route("/api/analysis")
    def retrieve_analysis() -> JSONDict:
        db = get_db()
        sort_column = request.args.get("sortColumn", "crystal_id")
        sort_order_desc = sort_order_to_descending(request.args.get("sortOrder", "asc"))
        filter_query = request.args.get("filterQuery", "")
        with db.connect() as conn:
            try:
                before = time.time()
                result = db.retrieve_analysis_results(
                    conn, filter_query, sort_column, sort_order_desc, limit=100
                )
                after = time.time()
                logger.info("Analysis query took %ss", int(after - before))

                return {
                    "analysisColumns": result.columns,
                    "totalRows": result.total_rows,
                    "totalDiffractions": result.total_diffractions,
                    "totalReductions": result.total_reductions,
                    "analysis": result.rows,
                    "sqlError": None,
                }
            except Exception as e:
                logger.exception(e)
                return {
                    "analysisColumns": [],
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

    return app


if __name__ == "__main__":
    create_app().run(port=5000)
