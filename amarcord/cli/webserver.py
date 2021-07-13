import datetime
import json
import logging
import os
import time
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from threading import Thread
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple

import sqlalchemy as sa
from flask import Flask
from flask import Response
from flask import current_app
from flask import g
from flask import request
from flask_cors import CORS
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import Label
from werkzeug.exceptions import BadRequest
from werkzeug.exceptions import HTTPException
from werkzeug.utils import redirect

from amarcord.amici.p11.alembic import upgrade_to_head
from amarcord.amici.p11.db import Beamline
from amarcord.amici.p11.db import DiffractionType
from amarcord.amici.p11.db import PuckType
from amarcord.amici.p11.db import ReductionMethod
from amarcord.amici.p11.db import table_crystals
from amarcord.amici.p11.db import table_data_reduction
from amarcord.amici.p11.db import table_dewar_lut
from amarcord.amici.p11.db import table_diffractions
from amarcord.amici.p11.db import table_job_to_diffraction
from amarcord.amici.p11.db import table_job_to_reduction
from amarcord.amici.p11.db import table_jobs
from amarcord.amici.p11.db import table_pucks
from amarcord.amici.p11.db import table_tools
from amarcord.locker import Locker
from amarcord.modules.dbcontext import Connection
from amarcord.modules.dbcontext import CreationMode
from amarcord.modules.dbcontext import DBContext
from amarcord.modules.json import JSONArray
from amarcord.modules.json import JSONDict
from amarcord.modules.json import JSONValue
from amarcord.workflows.command_line import CommandLine
from amarcord.workflows.command_line import parse_command_line
from amarcord.workflows.job_controller_factory import LocalJobControllerConfig
from amarcord.workflows.job_controller_factory import create_job_controller
from amarcord.workflows.job_controller_factory import parse_job_controller
from amarcord.workflows.job_status import JobStatus
from amarcord.workflows.workflow_synchronize import check_jobs

AMARCORD_DB_URL = "AMARCORD_DB_URL"
AMARCORD_JOB_CONTROLLER = "AMARCORD_JOB_CONTROLLER"

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WebserverDB:
    context: DBContext
    connection: Connection
    table_tools: sa.Table
    table_pucks: sa.Table
    table_crystals: sa.Table
    table_diffractions: sa.Table
    table_data_reductions: sa.Table
    table_jobs: sa.Table
    table_job_to_diffraction: sa.Table
    table_job_to_reduction: sa.Table

    def __enter__(self) -> "WebserverDB":
        return self

    def __exit__(self, _type: Any, value: Any, tb: Any) -> None:
        self.connection.close()


def _create_test_db(db: WebserverDB, test_files_dir: Path) -> None:
    crystals = db.connection.execute(
        sa.select([db.table_crystals.c.crystal_id])
    ).fetchone()
    if crystals is not None:
        return
    logger.info("First DB access, creating in-memory mock")
    for crystal in ["PLpr_7309_0", "PLpr_7309_1"]:
        db.connection.execute(sa.insert(db.table_crystals).values(crystal_id=crystal))
        db.connection.execute(
            sa.insert(db.table_diffractions).values(
                crystal_id=crystal,
                run_id=1,
                diffraction=DiffractionType.success,
                data_raw_filename_pattern=f"{test_files_dir}/{crystal}/{crystal}_001/*.cbf",
            )
        )
    db.connection.execute(
        sa.insert(db.table_tools).values(
            name="xia2",
            executable_path=str(
                Path().absolute() / "workflow-data" / "local-dummy-workflow.sh"
            ),
            extra_files=[
                str(Path().absolute() / "workflow-data" / "dummy-amarcord-output.json")
            ],
            command_line="${diffraction.path} ${testarg}",
            description="description",
        )
    )


def _create_db(db_url: str) -> WebserverDB:
    if "AMARCORD_DO_MIGRATIONS" in os.environ:
        upgrade_to_head(db_url)
    dbcontext = DBContext(db_url)
    pucks = table_pucks(dbcontext.metadata)
    crystals = table_crystals(dbcontext.metadata, pucks)
    tools = table_tools(dbcontext.metadata)
    diffs = table_diffractions(dbcontext.metadata, crystals)
    reductions = table_data_reduction(dbcontext.metadata, crystals)
    jobs = table_jobs(dbcontext.metadata, tools)
    reduction_jobs = table_job_to_diffraction(dbcontext.metadata, jobs, crystals, diffs)
    job_reductions = table_job_to_reduction(
        dbcontext.metadata, reduction_jobs, reductions
    )
    if db_url.startswith("sqlite://"):
        dbcontext.create_all(CreationMode.CHECK_FIRST)
    db = WebserverDB(
        dbcontext,
        dbcontext.connect(),
        tools,
        pucks,
        crystals,
        diffs,
        reductions,
        jobs,
        reduction_jobs,
        job_reductions,
    )
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
            with _create_db(db_url) as db:
                check_jobs(
                    job_controller,
                    db.connection,
                    db.table_tools,
                    db.table_jobs,
                    db.table_job_to_diffraction,
                    db.table_job_to_reduction,
                    db.table_diffractions,
                    db.table_data_reductions,
                )
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


def get_db() -> WebserverDB:
    if "db" not in g:
        # pylint: disable=assigning-non-slot
        g.db = _create_db(current_app.config["AMARCORD_DB_URL"])

    # pylint: disable=useless-return
    return g.db


# pylint: disable=useless-return
def close_db(_e: Optional[BaseException] = None) -> Response:
    db: Optional[WebserverDB] = g.pop("db", None)

    if db is not None:
        db.connection.close()

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

    def _retrieve_dewar_table(conn: Connection, dewar_lut: sa.Table) -> JSONDict:
        return {
            "dewarTable": [
                {"puckId": row[0], "dewarPosition": row[1]}
                for row in conn.execute(
                    sa.select(
                        [dewar_lut.c.puck_id, dewar_lut.c.dewar_position]
                    ).order_by(dewar_lut.c.dewar_position)
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

    def _convert_command_line(c: CommandLine) -> JSONArray:
        return [{"name": x.name, "type": x.type_.value} for x in c.inputs]

    def _retrieve_tools(conn: Connection, tools: sa.Table) -> JSONDict:
        return {
            "tools": [
                {
                    "toolId": row["id"],
                    "created": row["created"],
                    "name": row["name"],
                    "executablePath": row["executable_path"],
                    "extraFiles": row["extra_files"],
                    "commandLine": row["command_line"],
                    "description": row["description"],
                    "inputs": _convert_command_line(
                        parse_command_line(row["command_line"])
                    ),
                }
                for row in conn.execute(
                    sa.select(
                        [
                            tools.c.id,
                            tools.c.created,
                            tools.c.name,
                            tools.c.executable_path,
                            tools.c.extra_files,
                            tools.c.command_line,
                            tools.c.description,
                        ]
                    )
                ).fetchall()
            ]
        }

    @app.route("/api/workflows/tools")
    def retrieve_tools() -> JSONDict:
        db = get_db()
        return _retrieve_tools(db.connection, db.table_tools)

    @app.route("/api/pucks")
    def retrieve_pucks() -> JSONDict:
        db = get_db()
        return _retrieve_pucks(db.connection, db.table_pucks)

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
        )
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
        dbcontext = DBContext(os.environ[AMARCORD_DB_URL])
        crystals = table_crystals(dbcontext.metadata, table_pucks(dbcontext.metadata))
        diffractions = table_diffractions(dbcontext.metadata, crystals)
        with dbcontext.connect() as conn:
            return _retrieve_diffractions(conn, diffractions, crystals, puck_id)

    @app.post("/api/diffraction/<puck_id>")
    def add_diffraction(puck_id: str) -> JSONDict:
        dbcontext = DBContext(os.environ[AMARCORD_DB_URL])
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
        dbcontext = DBContext(os.environ[AMARCORD_DB_URL])
        dewar_lut = table_dewar_lut(dbcontext.metadata, table_pucks(dbcontext.metadata))
        with dbcontext.connect() as conn:
            conn.execute(
                sa.insert(dewar_lut).values(
                    dewar_position=dewarPosition, puck_id=puckId
                )
            )
            return _retrieve_dewar_table(conn, dewar_lut)

    @app.route("/api/dewar")
    def retrieve_dewar_table() -> JSONDict:
        dbcontext = DBContext(os.environ[AMARCORD_DB_URL])
        dewar_lut = table_dewar_lut(dbcontext.metadata, table_pucks(dbcontext.metadata))
        with dbcontext.connect() as conn:
            return _retrieve_dewar_table(conn, dewar_lut)

    @app.delete("/api/workflows/tools/<int:tool_id>")
    def remove_tool(tool_id: int) -> JSONDict:
        db = get_db()
        db.connection.execute(
            sa.delete(db.table_tools).where(db.table_tools.c.id == tool_id)
        )
        return _retrieve_tools(db.connection, db.table_tools)

    @app.delete("/api/pucks/<puck_id>")
    def remove_puck(puck_id: str) -> JSONDict:
        dbcontext = DBContext(os.environ[AMARCORD_DB_URL])
        pucks = table_pucks(dbcontext.metadata)
        crystals = table_crystals(dbcontext.metadata, pucks)
        with dbcontext.connect() as conn:
            conn.execute(sa.delete(pucks).where(pucks.c.puck_id == puck_id))
            return _retrieve_sample(conn, pucks, crystals)

    @app.delete("/api/crystals/<crystal_id>")
    def remove_crystal(crystal_id: str) -> JSONDict:
        dbcontext = DBContext(os.environ[AMARCORD_DB_URL])
        pucks = table_pucks(dbcontext.metadata)
        crystals = table_crystals(dbcontext.metadata, pucks)
        with dbcontext.connect() as conn:
            conn.execute(sa.delete(crystals).where(crystals.c.crystal_id == crystal_id))
            return _retrieve_sample(conn, pucks, crystals)

    @app.delete("/api/dewar/<int:position>")
    def remove_single_dewar_entry(position: int) -> JSONDict:
        dbcontext = DBContext(os.environ[AMARCORD_DB_URL])
        dewar_lut = table_dewar_lut(dbcontext.metadata, table_pucks(dbcontext.metadata))
        with dbcontext.connect() as conn:
            conn.execute(
                sa.delete(dewar_lut).where(dewar_lut.c.dewar_position == position)
            )
            return _retrieve_dewar_table(conn, dewar_lut)

    @app.delete("/api/dewar")
    def remove_dewar_lut() -> JSONDict:
        dbcontext = DBContext(os.environ[AMARCORD_DB_URL])
        dewar_lut = table_dewar_lut(dbcontext.metadata, table_pucks(dbcontext.metadata))
        with dbcontext.connect() as conn:
            conn.execute(sa.delete(dewar_lut))
            return _retrieve_dewar_table(conn, dewar_lut)

    def sort_order_to_descending(so: str) -> bool:
        if so == "asc":
            return False
        if so == "desc":
            return True
        raise BadRequest(f'invalid sort order "{so}"')

    @app.get("/api/workflows/jobs")
    def list_jobs() -> JSONDict:
        def convert_job(j: Dict[str, Any]) -> JSONDict:
            return {
                "jobId": j["id"],
                "started": j["started"],
                "queued": j["queued"],
                "stopped": j["stopped"],
                "status": j["status"].value,
                "failureReason": j["failure_reason"],
                "metadata": j["metadata"],
                "outputDir": j["output_directory"],
                "runId": j["run_id"],
                "crystalId": j["crystal_id"],
                "tool": j["tool"],
                "toolInputs": j["tool_inputs"],
            }

        db = get_db()
        rjc = db.table_jobs.c
        return {
            "jobs": [
                convert_job(j)
                for j in db.connection.execute(
                    sa.select(
                        [
                            rjc.id,
                            rjc.started,
                            rjc.queued,
                            rjc.stopped,
                            rjc.status,
                            rjc.failure_reason,
                            rjc.output_directory,
                            db.table_job_to_diffraction.c.run_id,
                            db.table_job_to_diffraction.c.crystal_id,
                            rjc.metadata,
                            db.table_tools.c.name.label("tool"),
                            rjc.tool_inputs,
                        ]
                    )
                    .select_from(
                        db.table_jobs.join(db.table_tools).outerjoin(
                            db.table_job_to_diffraction
                        )
                    )
                    .order_by(rjc.started.desc())
                )
            ]
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

        inserted_jobs: List[int] = []
        with db.connection.begin():
            query, _all_columns = _analysis_filter_query(
                db, filter_query, sort_column=None, sort_order_desc=False
            )
            processed_diffractions: Set[Tuple[str, int]] = set()
            for row in db.connection.execute(query):
                if len(processed_diffractions) == limit:
                    break
                crystal_id: str = row["diff_crystal_id"]
                run_id: int = row["diff_run_id"]

                if (crystal_id, run_id) not in processed_diffractions:
                    processed_diffractions.add((crystal_id, run_id))
                    result = db.connection.execute(
                        sa.insert(db.table_jobs).values(
                            status=JobStatus.QUEUED,
                            queued=datetime.datetime.utcnow(),
                            tool_id=tool_id,
                            tool_inputs=inputs,
                        )
                    )
                    job_id = result.inserted_primary_key[0]
                    db.connection.execute(
                        sa.insert(db.table_job_to_diffraction).values(
                            run_id=run_id, crystal_id=crystal_id, job_id=job_id
                        )
                    )
                    inserted_jobs.append(job_id)

        return {"jobIds": inserted_jobs}

    @app.post("/api/workflows/tools")
    def add_tool() -> JSONDict:
        db = get_db()
        db.connection.execute(
            sa.insert(db.table_tools).values(
                name=request.form["name"],
                executable_path=request.form["executablePath"],
                extra_files=json.loads(request.form["extraFiles"]),
                command_line=request.form["commandLine"],
                description=request.form["description"],
            )
        )
        return _retrieve_tools(db.connection, db.table_tools)

    @app.post("/api/workflows/tools/<int:tool_id>")
    def update_tool(tool_id: int) -> JSONDict:
        db = get_db()
        db.connection.execute(
            sa.update(db.table_tools)
            .values(
                name=request.form["name"],
                executable_path=request.form["executablePath"],
                extra_files=json.loads(request.form["extraFiles"]),
                command_line=request.form["commandLine"],
                description=request.form["description"],
            )
            .where(db.table_tools.c.id == tool_id)
        )
        return _retrieve_tools(db.connection, db.table_tools)

    @app.post("/api/crystals")
    def add_crystal() -> JSONDict:
        dbcontext = DBContext(os.environ[AMARCORD_DB_URL])
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
        dbcontext = DBContext(os.environ[AMARCORD_DB_URL])
        with dbcontext.connect() as conn:
            pucks = table_pucks(dbcontext.metadata)
            conn.execute(
                sa.insert(pucks).values(
                    puck_id=request.form["puckId"], puck_type=PuckType.UNI, owner="gui"
                )
            )
            return _retrieve_pucks(conn, pucks)

    def _retrieve_sample(
        conn: Connection, pucks: sa.Table, crystals: sa.Table
    ) -> JSONDict:
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
        dbcontext = DBContext(os.environ[AMARCORD_DB_URL])
        with dbcontext.connect() as conn:
            pucks = table_pucks(dbcontext.metadata)
            crystals = table_crystals(dbcontext.metadata, pucks, schema=None)
            return _retrieve_sample(conn, pucks, crystals)

    def _analysis_filter_query(
        db: WebserverDB,
        filter_query: str,
        sort_column: Optional[str],
        sort_order_desc: bool,
    ) -> Tuple[Select, List[Label]]:
        crystal_columns = [
            db.table_crystals.c.crystal_id.label("crystals_crystal_id"),
            db.table_crystals.c.created.label("crystals_created"),
        ]
        diffraction_columns = [
            c.label("diff_" + c.name) for c in db.table_diffractions.c
        ]
        reduction_columns = [
            c.label("dr_" + c.name) for c in db.table_data_reductions.c
        ]
        all_columns = (
            crystal_columns
            + diffraction_columns
            + reduction_columns
            + [
                db.table_jobs.c.id.label("jobs_id"),
                db.table_jobs.c.tool_inputs.label("jobs_tool_inputs"),
                db.table_tools.c.name.label("tools_name"),
            ]
        )
        query = (
            sa.select(all_columns)
            .select_from(
                db.table_crystals.outerjoin(db.table_diffractions)
                .outerjoin(
                    db.table_data_reductions,
                    onclause=sa.and_(
                        db.table_data_reductions.c.run_id
                        == db.table_diffractions.c.run_id,
                        db.table_diffractions.c.crystal_id
                        == db.table_data_reductions.c.crystal_id,
                    ),
                )
                .outerjoin(db.table_job_to_reduction)
                .outerjoin(db.table_jobs)
                .outerjoin(db.table_tools)
            )
            .where(sa.text(filter_query))
        )
        if sort_column is not None:
            query = query.order_by(
                sa.desc(sort_column) if sort_order_desc else sort_column
            )
        return query, all_columns

    @app.route("/api/analysis")
    def retrieve_analysis() -> JSONDict:
        db = get_db()
        sort_column = request.args.get("sortColumn", "crystal_id")
        sort_order_desc = sort_order_to_descending(request.args.get("sortOrder", "asc"))
        filter_query = request.args.get("filterQuery", "")
        all_columns: List[Label] = []
        try:
            results: List[Any] = []
            number_of_results = 0
            diffractions: Set[Tuple[str, int]] = set()

            before = time.time()
            query, all_columns = _analysis_filter_query(
                db, filter_query, sort_column, sort_order_desc
            )
            for row in db.connection.execute(query):
                number_of_results += 1
                if len(results) < 100:
                    results.append(row)
                diffractions.add((row["diff_crystal_id"], row["diff_run_id"]))
            after = time.time()
            logger.info("Analysis query took %ss", int(after - before))

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
                "totalRows": number_of_results,
                "totalDiffractions": len(diffractions),
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

    return app


if __name__ == "__main__":
    create_app().run(port=5000)
