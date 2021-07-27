import datetime
import json
import os
from pathlib import Path

from amarcord.modules.dbcontext import CreationMode
from amarcord.modules.dbcontext import DBContext
from amarcord.newdb.db_crystal import DBCrystal
from amarcord.newdb.db_data_reduction import DBDataReduction
from amarcord.newdb.db_diffraction import DBDiffraction
from amarcord.newdb.db_job import DBJob
from amarcord.newdb.db_refinement import DBRefinement
from amarcord.newdb.db_tool import DBTool
from amarcord.newdb.diffraction_type import DiffractionType
from amarcord.newdb.newdb import NewDB
from amarcord.newdb.reduction_method import ReductionMethod
from amarcord.newdb.refinement_method import RefinementMethod
from amarcord.newdb.tables import DBTables
from amarcord.workflows.job_status import JobStatus


def test_get_jobs(client):
    # This just tests if the jobs SQL statement doesn't fail, nothing more.
    rvjson = client.get("/api/workflows/jobs").get_json()
    assert rvjson["jobs"] == []


def test_get_analysis_only_one_crystal(client):
    # Insert a single crystal and check that the analysis query only returns that one crystal
    dbcontext = DBContext(os.environ["AMARCORD_DB_URL"])
    db = NewDB(
        dbcontext,
        DBTables(
            dbcontext.metadata,
            with_tools=True,
            with_estimated_resolution=False,
            normal_schema=None,
            analysis_schema=None,
        ),
    )
    dbcontext.create_all(CreationMode.DONT_CHECK)

    with db.connect() as conn:
        db.insert_crystal(conn, DBCrystal("cid"))
        # db.insert_diffraction(
        #     conn, DBDiffraction("cid", run_id=1, diffraction=DiffractionType.success)
        # )

    rvjson = client.get("/api/analysis").get_json()

    assert rvjson["analysis"] != []
    assert rvjson["analysisColumns"] != []
    assert rvjson["sqlError"] is None


def test_get_analysis_one_crystal_one_diffraction_one_reduction_no_jobs(client):
    # Test analysis query with one crystal, one diffraction and one reduction, but the reduction didn't come from
    # a job, but was inserted otherwise.
    dbcontext = DBContext(os.environ["AMARCORD_DB_URL"])
    db = NewDB(
        dbcontext,
        DBTables(
            dbcontext.metadata,
            with_tools=True,
            with_estimated_resolution=False,
            normal_schema=None,
            analysis_schema=None,
        ),
    )
    dbcontext.create_all(CreationMode.DONT_CHECK)

    with db.connect() as conn:
        db.insert_crystal(conn, DBCrystal("cid"))
        db.insert_diffraction(
            conn, DBDiffraction("cid", run_id=1, diffraction=DiffractionType.success)
        )
        DATA_REDUCTION_ID = 1
        db.insert_data_reduction(
            conn,
            DBDataReduction(
                data_reduction_id=DATA_REDUCTION_ID,
                crystal_id="cid",
                run_id=1,
                analysis_time=datetime.datetime.utcnow(),
                folder_path=Path("/tmp"),
                method=ReductionMethod.OTHER,
            ),
        )

    rvjson = client.get("/api/analysis").get_json()

    assert len(rvjson["analysis"]) == 1
    assert (
        rvjson["analysis"][0][rvjson["analysisColumns"].index("dr_data_reduction_id")]
        == DATA_REDUCTION_ID
    )
    assert rvjson["analysisColumns"] != []
    assert rvjson["sqlError"] is None


def test_get_analysis_one_crystal_one_diffraction_one_reduction_from_job(client):
    # Test analysis query with one crystal, one diffraction and one reduction. The reduction came from a job.
    dbcontext = DBContext(os.environ["AMARCORD_DB_URL"])
    db = NewDB(
        dbcontext,
        DBTables(
            dbcontext.metadata,
            with_tools=True,
            with_estimated_resolution=False,
            normal_schema=None,
            analysis_schema=None,
        ),
    )
    dbcontext.create_all(CreationMode.DONT_CHECK)

    with db.connect() as conn:
        db.insert_crystal(conn, DBCrystal("cid"))
        TOOL_NAME = "toolname"
        tool_id = db.insert_tool(
            conn,
            DBTool(
                id=None,
                name=TOOL_NAME,
                executable_path=Path("/tmp"),
                extra_files=[],
                command_line="",
                description="",
                inputs=None,
                created=None,
            ),
        )
        db.insert_diffraction(
            conn, DBDiffraction("cid", run_id=1, diffraction=DiffractionType.success)
        )
        DATA_REDUCTION_ID = 1
        db.insert_data_reduction(
            conn,
            DBDataReduction(
                data_reduction_id=DATA_REDUCTION_ID,
                crystal_id="cid",
                run_id=1,
                analysis_time=datetime.datetime.utcnow(),
                folder_path=Path("/tmp"),
                method=ReductionMethod.OTHER,
            ),
        )
        job_id = db.insert_job(
            conn,
            DBJob(
                id=None,
                queued=datetime.datetime.utcnow(),
                status=JobStatus.COMPLETED,
                tool_id=tool_id,
                tool_inputs={},
            ),
        )
        db.insert_job_to_diffraction(conn, job_id, "cid", 1)

    rvjson = client.get("/api/analysis").get_json()

    assert len(rvjson["analysis"]) == 1
    assert (
        rvjson["analysis"][0][rvjson["analysisColumns"].index("dr_data_reduction_id")]
        == DATA_REDUCTION_ID
    )
    assert (
        rvjson["analysis"][0][rvjson["analysisColumns"].index("red_jobs_tool_name")]
        == TOOL_NAME
    )
    assert rvjson["analysisColumns"] != []
    assert rvjson["sqlError"] is None


def test_get_analysis_one_crystal_one_diffraction_one_reduction_one_refinement(client):
    # Test analysis query with one crystal, one diffraction, one reduction, one refinement
    dbcontext = DBContext(os.environ["AMARCORD_DB_URL"])
    db = NewDB(
        dbcontext,
        DBTables(
            dbcontext.metadata,
            with_tools=True,
            with_estimated_resolution=False,
            normal_schema=None,
            analysis_schema=None,
        ),
    )
    dbcontext.create_all(CreationMode.CHECK_FIRST)

    with db.connect() as conn:
        db.insert_crystal(conn, DBCrystal("cid"))
        db.insert_diffraction(
            conn, DBDiffraction("cid", run_id=1, diffraction=DiffractionType.success)
        )
        data_reduction_id = db.insert_data_reduction(
            conn,
            DBDataReduction(
                data_reduction_id=None,
                crystal_id="cid",
                run_id=1,
                analysis_time=datetime.datetime.utcnow(),
                folder_path=Path("/tmp"),
                method=ReductionMethod.OTHER,
            ),
        )
        assert not db.retrieve_refinements(conn)
        refinement_id = db.insert_refinement(
            conn,
            DBRefinement(
                refinement_id=None,
                data_reduction_id=data_reduction_id,
                analysis_time=datetime.datetime.utcnow(),
                folder_path=Path("/tmp"),
                method=RefinementMethod.OTHER,
            ),
        )
        assert len(db.retrieve_refinements(conn)) == 1
        assert refinement_id is not None

    rvjson = client.get("/api/analysis").get_json()

    assert len(rvjson["analysis"]) == 1
    assert (
        rvjson["analysis"][0][rvjson["analysisColumns"].index("ref_refinement_id")]
        == refinement_id
    )
    assert rvjson["analysisColumns"] != []
    assert rvjson["sqlError"] is None


def test_get_empty_analysis(client):
    # This just tests if the analysis SQL statement doesn't fail, nothing more.
    rvjson = client.get("/api/analysis").get_json()

    assert rvjson["analysis"] == []
    assert rvjson["analysisColumns"] != []
    assert rvjson["sqlError"] is None


def test_add_and_get_tool(client):
    # Assume empty tools list
    rv = client.get("/api/workflows/tools")
    assert rv.get_json() == {"tools": []}

    # Add another tool
    result = client.post(
        "/api/workflows/tools",
        data=dict(
            name="newtool",
            executablePath="/usr/bin/test",
            extraFiles=json.dumps(["/tmp/testfile"]),
            commandLine="foo${bar}baz",
            description="lol",
        ),
    )

    assert result.status_code == 200
    result_json = result.get_json()
    assert result_json["tools"]
    assert result_json["tools"][0]["toolId"] != 0
    assert result_json["tools"][0]["name"] == "newtool"
