import datetime
from dataclasses import replace
from pathlib import Path

from amarcord.modules.dbcontext import CreationMode
from amarcord.modules.dbcontext import DBContext
from amarcord.newdb.db_crystal import DBCrystal
from amarcord.newdb.db_data_reduction import DBDataReduction
from amarcord.newdb.db_dewar_lut import DBDewarLUT
from amarcord.newdb.db_diffraction import DBDiffraction
from amarcord.newdb.db_puck import DBPuck
from amarcord.newdb.db_tool import DBTool
from amarcord.newdb.diffraction_type import DiffractionType
from amarcord.newdb.newdb import NewDB
from amarcord.newdb.puck_type import PuckType
from amarcord.newdb.reduction_method import ReductionMethod
from amarcord.newdb.tables import DBTables

RUN_ID = 1

OWNER = "test"

PID = "pid"
PID2 = "pid2"

CRYSTAL_ID = "crystal1"


def test_crystal_crud() -> None:
    dbcontext = DBContext("sqlite://", echo=False)
    db = NewDB(dbcontext, DBTables(dbcontext.metadata))
    dbcontext.create_all(creation_mode=CreationMode.DONT_CHECK)

    with db.connect() as conn:
        assert not db.has_crystals(conn)

        db.insert_crystal(conn, DBCrystal(CRYSTAL_ID))

        assert db.has_crystals(conn)
        assert db.crystal_exists(conn, CRYSTAL_ID)
        assert not db.crystal_exists(conn, CRYSTAL_ID + "shit")
        assert len(db.retrieve_sample_data(conn).crystals) == 1
        assert db.retrieve_sample_data(conn).crystals[0].crystal_id == CRYSTAL_ID

        db.remove_crystal(conn, CRYSTAL_ID)

        assert not db.has_crystals(conn)


def test_puck_crud() -> None:
    dbcontext = DBContext("sqlite://", echo=False)
    db = NewDB(dbcontext, DBTables(dbcontext.metadata))
    dbcontext.create_all(creation_mode=CreationMode.DONT_CHECK)

    with db.connect() as conn:
        db.insert_puck(conn, DBPuck(id=PID, puck_type=PuckType.UNI, owner=OWNER))

        assert len(db.retrieve_sample_data(conn).pucks) == 1
        assert db.retrieve_sample_data(conn).pucks[0].id == PID
        assert db.retrieve_sample_data(conn).pucks[0].puck_type == PuckType.UNI
        assert db.retrieve_sample_data(conn).pucks[0].owner == OWNER

        db.remove_puck(conn, PID)

        assert len(db.retrieve_sample_data(conn).pucks) == 0


def test_diffractions_crud() -> None:
    dbcontext = DBContext("sqlite://", echo=False)
    db = NewDB(dbcontext, DBTables(dbcontext.metadata))
    dbcontext.create_all(creation_mode=CreationMode.DONT_CHECK)

    with db.connect() as conn:
        db.insert_crystal(conn, DBCrystal(CRYSTAL_ID))
        db.insert_diffraction(
            conn,
            DBDiffraction(CRYSTAL_ID, RUN_ID, DiffractionType.success, pinhole="abc"),
        )

        assert db.has_diffractions(conn, CRYSTAL_ID, RUN_ID)
        assert len(db.retrieve_analysis_diffractions(conn, "")) == 1

        db.update_diffraction(
            conn,
            DBDiffraction(CRYSTAL_ID, RUN_ID, DiffractionType.success, pinhole="xyz"),
        )

        assert len(db.retrieve_analysis_diffractions(conn, "")) == 1


def test_tools_crud() -> None:
    dbcontext = DBContext("sqlite://", echo=False)
    db = NewDB(dbcontext, DBTables(dbcontext.metadata))
    dbcontext.create_all(creation_mode=CreationMode.DONT_CHECK)

    with db.connect() as conn:
        tool = DBTool(
            id=None,
            name="tool_name",
            executable_path=Path("foo/bar"),
            extra_files=[Path("a"), Path("b")],
            command_line="command-line",
            description="desc",
            inputs=None,
        )
        tool_id = db.insert_tool(
            conn,
            tool,
        )

        assert tool_id is not None

        assert len(db.retrieve_tools(conn)) == 1
        assert db.retrieve_tools(conn)[0].name == "tool_name"

        db.update_tool(conn, replace(tool, name="newname", id=tool_id))

        assert db.retrieve_tools(conn)[0].name == "newname"
        db.remove_tool(conn, tool_id)

        assert len(db.retrieve_tools(conn)) == 0


def test_dewar_crud() -> None:
    dbcontext = DBContext("sqlite://", echo=False)
    db = NewDB(dbcontext, DBTables(dbcontext.metadata))
    dbcontext.create_all(creation_mode=CreationMode.DONT_CHECK)

    with db.connect() as conn:
        db.insert_crystal(conn, DBCrystal(CRYSTAL_ID))
        db.insert_puck(conn, DBPuck(PID, PuckType.UNI, OWNER))
        db.insert_puck(conn, DBPuck(PID2, PuckType.UNI, OWNER))

        db.insert_dewar_table_entry(conn, DBDewarLUT(dewar_position=1, puck_id=PID))
        assert len(db.retrieve_dewar_table(conn)) == 1
        assert db.retrieve_dewar_table(conn)[0].dewar_position == 1
        assert db.retrieve_dewar_table(conn)[0].puck_id == PID

        db.insert_dewar_table_entry(conn, DBDewarLUT(dewar_position=2, puck_id=PID2))

        assert len(db.retrieve_dewar_table(conn)) == 2

        db.remove_dewar_table_entry(conn, dewar_position=1)

        assert len(db.retrieve_dewar_table(conn)) == 1
        assert db.retrieve_dewar_table(conn)[0].dewar_position == 2

        db.truncate_dewar_table(conn)

        assert len(db.retrieve_dewar_table(conn)) == 0


def test_reductions() -> None:
    dbcontext = DBContext("sqlite://", echo=False)
    db = NewDB(dbcontext, DBTables(dbcontext.metadata))
    dbcontext.create_all(creation_mode=CreationMode.DONT_CHECK)

    with db.connect() as conn:
        db.insert_crystal(conn, DBCrystal(CRYSTAL_ID))
        db.insert_diffraction(
            conn,
            DBDiffraction(CRYSTAL_ID, RUN_ID, DiffractionType.success),
        )
        db.insert_data_reduction(
            conn,
            DBDataReduction(
                data_reduction_id=None,
                crystal_id=CRYSTAL_ID,
                run_id=RUN_ID,
                analysis_time=datetime.datetime.utcnow(),
                method=ReductionMethod.OTHER,
                folder_path=Path("/tmp/"),
            ),
        )

        assert db.directory_has_reductions(conn, Path("/tmp/"))
        assert not db.directory_has_reductions(conn, Path("/tmp2/"))
        ar = db.retrieve_analysis_results(
            conn, "", sort_column=None, sort_order_desc=False, limit=None
        )
        assert len(ar.rows) == 1
        assert ar.rows[0][ar.columns.index("dr_data_reduction_id")] is not None
