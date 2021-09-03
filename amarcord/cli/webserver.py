import datetime
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
from flask.json import JSONEncoder
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
from amarcord.modules.json_checker import JSONChecker
from amarcord.newdb.alembic import upgrade_to_head
from amarcord.newdb.db_crystal import DBCrystal
from amarcord.newdb.db_data_reduction import DBDataReduction
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
from amarcord.newdb.reduction_method import ReductionMethod
from amarcord.newdb.reduction_simple_filter import ReductionSimpleFilter
from amarcord.newdb.tables import DBTables
from amarcord.newdb.tables import SeparateSchemata
from amarcord.workflows.command_line import CommandLine
from amarcord.workflows.job_controller_factory import LocalJobControllerConfig
from amarcord.workflows.job_controller_factory import create_job_controller
from amarcord.workflows.job_controller_factory import parse_job_controller
from amarcord.workflows.job_status import JobStatus
from amarcord.workflows.workflow_synchronize import bulk_start_jobs
from amarcord.workflows.workflow_synchronize import check_jobs

AMARCORD_DB_URL = "AMARCORD_DB_URL"
AMARCORD_JOB_CONTROLLER = "AMARCORD_JOB_CONTROLLER"

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)


class CustomJSONEncoder(JSONEncoder):
    def default(self, o):
        # The default ISO format for JSON encoding isn't well-parseable, better to use good olde ISO!
        if isinstance(o, datetime.datetime):
            return o.isoformat(sep=" ", timespec="seconds")
        return JSONEncoder.default(self, o)


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
            db.insert_data_reduction(
                conn,
                DBDataReduction(
                    data_reduction_id=None,
                    crystal_id=crystal,
                    run_id=1,
                    analysis_time=datetime.datetime.utcnow(),
                    folder_path=Path(
                        f"{test_files_dir}/reductions/{crystal}/{crystal}_001"
                    ),
                    mtz_path=Path(
                        f"{test_files_dir}/reductions/{crystal}/{crystal}_001/test.mtz"
                    ),
                    method=ReductionMethod.STARANISO,
                    resolution_cc=1.0,
                    resolution_isigma=2.0,
                    a=30.0,
                    b=40.0,
                    c=50.0,
                    alpha=90.0,
                    beta=120.0,
                    gamma=240.0,
                    space_group=152,
                    isigi=1.0,
                    rmeas=1.0,
                    cchalf=1.0,
                    rfactor=1.0,
                    wilson_b=20.0,
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
                command_line="${Data_Reduction.mtz_path} ${Data_Reduction.resolution_cc}",
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
        schemata=SeparateSchemata.from_two_optionals(
            os.environ.get("AMARCORD_MAIN_SCHEMA", None),
            os.environ.get("AMARCORD_ANALYSIS_SCHEMA", None),
        ),
    )
    if db_url.startswith("sqlite://"):
        dbcontext.create_all(CreationMode.CHECK_FIRST)
    tables.load_from_engine(dbcontext.engine)
    # Interesting info, but not on every create_db
    logger.debug(
        "dynamically loaded tables from engine for %s, crystals columns: %s",
        db_url,
        ",".join(c.name for c in tables.crystals.columns),
    )
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
    app.json_encoder = CustomJSONEncoder
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
        """ Used in the "Administer tools" GUI """
        db = get_db()
        with db.connect() as conn:
            return _retrieve_tools(conn, db)

    @app.route("/api/pucks")
    def retrieve_pucks() -> JSONDict:
        """ Used in the "Beamline" GUI """
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
        """ Used in the "Beamline" GUI """
        db = get_db()
        with db.connect() as conn:
            return _retrieve_diffractions(conn, db, puck_id)

    @app.post("/api/diffraction/<puck_id>")
    def add_diffraction(puck_id: str) -> JSONDict:
        """ Used in the "Beamline" GUI """
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
        """ Used in the "Beamline" GUI """
        db = get_db()
        with db.connect() as conn:
            db.insert_dewar_table_entry(conn, DBDewarLUT(dewar_position, puck_id))
            return _retrieve_dewar_table(conn, db)

    @app.route("/api/dewar")
    def retrieve_dewar_table() -> JSONDict:
        """ Used in the "Beamline" GUI """
        db = get_db()
        with db.connect() as conn:
            return _retrieve_dewar_table(conn, db)

    @app.delete("/api/workflows/tools/<int:tool_id>")
    def remove_tool(tool_id: int) -> JSONDict:
        """ Used in the "Administer tools" GUI """
        db = get_db()
        with db.connect() as conn:
            db.remove_tool(conn, tool_id)
            return _retrieve_tools(conn, db)

    @app.delete("/api/pucks/<puck_id>")
    def remove_puck(puck_id: str) -> JSONDict:
        # Used in the "Sample" GUI
        db = get_db()
        with db.connect() as conn:
            db.remove_puck(conn, puck_id)
            return _retrieve_sample(conn, db)

    @app.delete("/api/crystals/<crystal_id>")
    def remove_crystal(crystal_id: str) -> JSONDict:
        # Used in the "Sample" GUI
        db = get_db()
        with db.connect() as conn:
            db.remove_crystal(conn, crystal_id)
            return _retrieve_sample(conn, db)

    @app.delete("/api/dewar/<int:position>")
    def remove_single_dewar_entry(position: int) -> JSONDict:
        # Used in the "Beamline" GUI
        db = get_db()
        with db.connect() as conn:
            db.remove_dewar_table_entry(conn, position)
            return _retrieve_dewar_table(conn, db)

    @app.delete("/api/dewar")
    def remove_dewar_lut() -> JSONDict:
        # Used in the "Beamline" GUI
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

    @app.get("/api/workflows/jobs/<int:job_id>")
    def retrieve_job(job_id: int) -> JSONDict:
        # Used in the "Job details" GUI
        db = get_db()
        with db.connect() as conn:
            job = db.retrieve_job(conn, job_id)
            return {
                "job": {
                    "jobId": job_id,
                    "queued": job.queued,
                    "status": job.status.value,
                    "toolId": job.tool_id,
                    "toolInputs": job.tool_inputs,
                    "failureReason": job.failure_reason,
                    "comment": job.comment,
                    "outputDir": str(job.output_directory),
                    "lastStdout": job.last_stdout,
                    "lastStderr": job.last_stderr,
                    "metadata": job.metadata,
                    "started": job.started,
                    "stopped": job.stopped,
                }
            }

    # This is used in the jobs table and retrieves not only the jobs themselves, but also to what they are
    # attached to (diffractions/reductions)
    @app.get("/api/workflows/jobs")
    def list_jobs() -> JSONDict:
        """ Used in the "Job list" GUI """
        limit = int(request.args.get("limit", "10"))
        status_filter_str = request.args.get("statusFilter", None)
        since_str = request.args.get("since", None)

        since: Optional[datetime.datetime] = None
        if since_str is not None:
            hours_in_the_past: int
            if since_str == "last_day":
                hours_in_the_past = 24
            elif since_str == "last_week":
                hours_in_the_past = 7 * 24
            else:
                hours_in_the_past = 28 * 24
            since = datetime.datetime.utcnow() - datetime.timedelta(
                hours=hours_in_the_past
            )
        try:
            status_filter = (
                JobStatus(status_filter_str) if status_filter_str is not None else None
            )
        except:
            raise Exception(
                f'invalid job status "{status_filter_str}", valid stati are '
                + ", ".join(e.value for e in JobStatus)
            )

        def convert_job(j: DBJobWithInputsAndOutputs) -> JSONDict:
            return {
                "jobId": j.job.id,
                "started": j.job.started,
                "queued": j.job.queued,
                "stopped": j.job.stopped,
                "status": j.job.status.value,
                "comment": j.job.comment if j.job.comment else "",
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
                    "runId": j.io.run_id,
                    "crystalId": j.io.crystal_id,
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
                "jobs": [
                    convert_job(j)
                    for j in db.retrieve_jobs_with_attached(
                        conn, limit if limit != 0 else None, status_filter, since
                    )
                ]
            }

    def _safe_json_dict() -> JSONDict:
        json_content = request.get_json(force=True)
        assert isinstance(
            json_content, dict
        ), f"expected a dictionary for the request input, got {json_content}"
        return json_content

    # There were at one point two approaches to job starting: one used a filter query, which was "complicated" in a
    # way, so this one uses a special "filter" structure that's simpler than an arbitrary SQL query.
    @app.post("/api/workflows/jobs-simple/start/<int:tool_id>")
    def start_job_simple(tool_id: int) -> JSONDict:
        """ Used in the "Run tools" GUI """
        json_content = JSONChecker(_safe_json_dict(), "POST request")

        db = get_db()

        with db.connect() as conn:
            with conn.begin():
                return {
                    "jobIds": bulk_start_jobs(
                        conn,
                        db,
                        tool_id,
                        json_content.optional_str("comment"),
                        _parse_reduction_filter(json_content.d),
                        json_content.optional_int("limit"),
                        json_content.retrieve_safe_dict("inputs"),
                    )
                }

    @app.post("/api/workflows/jobs/start/<int:tool_id>")
    def start_job(tool_id: int) -> JSONDict:
        """ Not really used anymore """
        json_content = JSONChecker(_safe_json_dict(), "POST request")
        inputs = json_content.retrieve_safe_dict("inputs")
        filter_query = json_content.retrieve_safe_str("filter_query")
        limit = json_content.optional_int("limit")
        db = get_db()

        with db.connect() as conn:
            with conn.begin():
                return {
                    "jobIds": bulk_start_jobs(
                        conn,
                        db,
                        tool_id,
                        json_content.optional_str("comment"),
                        filter_query,
                        limit,
                        inputs,
                    )
                }

    @app.post("/api/workflows/tools")
    def add_tool() -> JSONDict:
        """ Used in the "Administer tools" GUI """
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
        """ Used in the "Administer tools" GUI """
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
        """ Used in the "Sample" GUI """
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
        """ Used in the "Sample" GUI """
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
        """ Used in the "Sample" GUI """
        db = get_db()
        with db.connect() as conn:
            return _retrieve_sample(conn, db)

    def _parse_reduction_filter(json_content: JSONDict) -> ReductionSimpleFilter:
        reduction_method_str = json_content.get("reductionMethod")
        only_non_refined = json_content.get("onlyNonrefined")
        crystal_filters = json_content.get("crystalFilters")

        if not isinstance(only_non_refined, bool):
            raise Exception(
                f'invalid "only_non_refined", expected bool, got {only_non_refined}'
            )

        if not isinstance(crystal_filters, dict):
            raise Exception(
                f'invalid "crystal_filters", expected dict, got {crystal_filters}'
            )

        reduction_method: Optional[ReductionMethod]
        try:
            if reduction_method_str is not None:
                reduction_method = ReductionMethod(reduction_method_str)
            else:
                reduction_method = None
        except:
            raise Exception(
                f'reduction method "{reduction_method_str}" not valid; valid methods are: '
                + ", ".join(x.value for x in ReductionMethod)
            )

        return ReductionSimpleFilter(
            reduction_method=reduction_method,
            only_non_refined=only_non_refined,
            crystal_filters=crystal_filters,
        )

    # Retrieves the number of reductions for a simple reduction filter. Used in the "Start tool" GUI
    @app.post("/api/reduction-count")
    def retrieve_reduction_count() -> JSONDict:
        """ Used in the "Run tools" GUI """
        db = get_db()
        with db.connect() as conn:
            return {
                "totalResults": len(
                    db.retrieve_data_reduction_ids_simple(
                        conn, filter_=_parse_reduction_filter(_safe_json_dict())
                    )
                )
            }

    @app.route("/api/crystal-filters")
    def retrieve_crystal_filters() -> JSONDict:
        """ Used in the "Run tools" GUI """
        db = get_db()

        with db.connect() as conn:
            return {"columnsWithValues": db.retrieve_crystal_column_values(conn)}

    @app.route("/api/analysis")
    def retrieve_analysis() -> JSONDict:
        """ Used in the "Analysis" GUI """
        db = get_db()
        sort_column = request.args.get("sortColumn", "crystal_id")
        sort_order_desc = sort_order_to_descending(request.args.get("sortOrder", "asc"))
        filter_query = request.args.get("filterQuery", "")
        with db.connect() as conn:
            try:
                before = time.time()
                result = db.retrieve_analysis_results(
                    conn, filter_query, sort_column, sort_order_desc, limit=500
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
