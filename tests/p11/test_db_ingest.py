from dataclasses import replace
from pathlib import Path

import sqlalchemy as sa
from pint import UnitRegistry

from amarcord.amici.p11.analyze_filesystem import P11Crystal
from amarcord.amici.p11.analyze_filesystem import P11Run
from amarcord.amici.p11.db_ingest import EIGER_16_M_DETECTOR_NAME
from amarcord.amici.p11.db_ingest import MetadataRetriever
from amarcord.amici.p11.db_ingest import empty_metadata_retriever
from amarcord.amici.p11.db_ingest import ingest_diffractions_for_crystals
from amarcord.amici.p11.db_ingest import insert_diffraction
from amarcord.amici.p11.parser import parse_p11_info_file
from amarcord.amici.p11.spreadsheet_reader import CrystalLine
from amarcord.cli.p11_filesystem_ingester import process_and_validate_with_spreadsheet
from amarcord.modules.dbcontext import CreationMode
from amarcord.modules.dbcontext import DBContext
from amarcord.newdb.db_crystal import DBCrystal
from amarcord.newdb.diffraction_type import DiffractionType
from amarcord.newdb.newdb import NewDB
from amarcord.newdb.tables import DBTables

DEFAULT_METADATA_RETRIEVER = MetadataRetriever(
    lambda _crystal_id, _run_id: DiffractionType.success,
    lambda _crystal_id, _run_id: "",
    lambda _crystal_id, _run_id: "",
    EIGER_16_M_DETECTOR_NAME,
)


def test_db_ingest_diffractions_successful(db) -> None:
    dbcontext = DBContext("sqlite://", echo=False)
    db = NewDB(
        dbcontext,
        _tables(dbcontext),
    )
    dbcontext.create_all(creation_mode=CreationMode.DONT_CHECK)

    with dbcontext.connect() as conn:
        crystal_id = "c1"

        db.insert_crystal(conn, DBCrystal(crystal_id))

        crystal_list = [
            P11Crystal(
                crystal_id,
                runs=[
                    P11Run(
                        1,
                        Path(__file__).parent,
                        parse_p11_info_file(
                            Path(__file__).parent / "info.txt", UnitRegistry()
                        ),
                        data_raw_filename_pattern=None,
                        microscope_image_filename_pattern=None,
                    )
                ],
            )
        ]
        has_warnings = ingest_diffractions_for_crystals(
            conn,
            db,
            crystal_list,
            insert_diffraction_if_not_exists=True,
            metadata_retriever=empty_metadata_retriever(EIGER_16_M_DETECTOR_NAME),
        )
        assert not has_warnings

    with dbcontext.connect() as conn:
        diffractions = db.retrieve_analysis_diffractions(conn, "")

        assert len(diffractions) == 1
        assert diffractions[0][0] == crystal_id


def _tables(dbcontext):
    return DBTables(
        dbcontext.metadata,
        with_tools=False,
        with_estimated_resolution=False,
        normal_schema=None,
        analysis_schema=None,
    )


def test_db_ingest_diffractions_crystal_in_filesystem_but_not_in_db(db) -> None:
    dbcontext = DBContext("sqlite://", echo=False)
    db = NewDB(dbcontext, _tables(dbcontext))
    dbcontext.create_all(creation_mode=CreationMode.DONT_CHECK)

    with dbcontext.connect() as conn:
        crystal_id = "c1"

        crystal_list = [
            P11Crystal(
                crystal_id,
                runs=[
                    P11Run(
                        1,
                        Path(__file__).parent,
                        parse_p11_info_file(
                            Path(__file__).parent / "info.txt", UnitRegistry()
                        ),
                        data_raw_filename_pattern=None,
                        microscope_image_filename_pattern=None,
                    )
                ],
            )
        ]
        has_warnings = ingest_diffractions_for_crystals(
            conn,
            db,
            crystal_list,
            insert_diffraction_if_not_exists=True,
            metadata_retriever=empty_metadata_retriever(EIGER_16_M_DETECTOR_NAME),
        )
        assert has_warnings

    with dbcontext.connect() as conn:
        diffraction_ids = conn.execute(
            sa.select([db.tables.diffs.c.crystal_id])
        ).fetchall()

        assert not diffraction_ids


def test_db_ingest_diffractions_diffraction_does_not_exist_and_not_add_it(db) -> None:
    dbcontext = DBContext("sqlite://", echo=False)
    db = NewDB(dbcontext, _tables(dbcontext))
    dbcontext.create_all(creation_mode=CreationMode.DONT_CHECK)

    with dbcontext.connect() as conn:
        crystal_id = "c1"
        db.insert_crystal(conn, DBCrystal(crystal_id))

        crystal_list = [
            P11Crystal(
                crystal_id,
                runs=[
                    P11Run(
                        1,
                        Path(__file__).parent,
                        parse_p11_info_file(
                            Path(__file__).parent / "info.txt", UnitRegistry()
                        ),
                        data_raw_filename_pattern=None,
                        microscope_image_filename_pattern=None,
                    )
                ],
            )
        ]
        diffraction_warnings = ingest_diffractions_for_crystals(
            conn,
            db,
            crystal_list,
            insert_diffraction_if_not_exists=False,
            metadata_retriever=empty_metadata_retriever(EIGER_16_M_DETECTOR_NAME),
        )
        assert not diffraction_warnings

    with dbcontext.connect() as conn:
        assert not db.retrieve_analysis_diffractions(conn, "")


def test_db_ingest_diffractions_update_diffraction_if_exists(db) -> None:
    dbcontext = DBContext("sqlite://", echo=False)
    db = NewDB(dbcontext, _tables(dbcontext))
    dbcontext.create_all(creation_mode=CreationMode.DONT_CHECK)

    with dbcontext.connect() as conn:
        crystal_id = "c1"
        db.insert_crystal(conn, DBCrystal(crystal_id))

        info_file = parse_p11_info_file(
            Path(__file__).parent / "info.txt", UnitRegistry()
        )
        run = P11Run(
            1,
            Path(__file__).parent,
            info_file,
            data_raw_filename_pattern=None,
            microscope_image_filename_pattern=None,
        )
        # "Pre-insert" the diffraction, using a different number of frames as an example field
        insert_diffraction(
            conn,
            db,
            crystal_id,
            replace(
                # See replacement here
                run,
                info_file=replace(run.info_file, frames=run.info_file.frames + 1),
            ),
            empty_metadata_retriever(EIGER_16_M_DETECTOR_NAME),
        )
        crystal_list = [
            P11Crystal(
                crystal_id,
                runs=[run],
            )
        ]
        has_warnings = ingest_diffractions_for_crystals(
            conn,
            db,
            crystal_list,
            insert_diffraction_if_not_exists=False,
            metadata_retriever=empty_metadata_retriever(EIGER_16_M_DETECTOR_NAME),
        )
        assert not has_warnings

    with dbcontext.connect() as conn:
        analysis_results = db.retrieve_analysis_results(conn, "", None, False, None)
        assert len(analysis_results.rows) == 1
        number_of_frame_column = analysis_results.columns.index("diff_number_of_frames")
        assert number_of_frame_column is not None

        # We have pre-inserted "frames+1" and now expected "frames" as the updated value
        assert analysis_results.rows[0][number_of_frame_column] == run.info_file.frames


def test_process_and_validate_with_spreadsheet_run_doesnt_match() -> None:
    spreadsheet_lines = [
        CrystalLine(1, "directory", "name", 1, DiffractionType.success, "comment", ""),
        CrystalLine(
            2, "directory2", "name2", 1, DiffractionType.success, "comment2", ""
        ),
    ]
    crystals = [
        # This crystal matches the first line of the spreadsheet, so it will be renamed from "directory"
        # to "name"
        P11Crystal(
            "directory",
            runs=[
                P11Run(
                    1,
                    Path(__file__).parent,
                    parse_p11_info_file(
                        Path(__file__).parent / "info.txt", UnitRegistry()
                    ),
                    data_raw_filename_pattern=None,
                    microscope_image_filename_pattern=None,
                )
            ],
        ),
        # This crystals matches, too, but not the run number, so it will cause a warning
        P11Crystal(
            "name",
            runs=[
                P11Run(
                    2,
                    Path(__file__).parent,
                    parse_p11_info_file(
                        Path(__file__).parent / "info.txt", UnitRegistry()
                    ),
                    data_raw_filename_pattern=None,
                    microscope_image_filename_pattern=None,
                )
            ],
        ),
    ]
    new_crystals, has_warnings = process_and_validate_with_spreadsheet(
        spreadsheet_lines, crystals
    )
    assert len(new_crystals) == 1
    assert new_crystals[0].crystal_id == "name"
    assert has_warnings


def test_process_and_validate_with_spreadsheet_more_lines_in_spreadsheet_than_filesystem() -> None:
    # Here we have two crystals, but only one on the filesystem. This should cause warnings.
    spreadsheet_lines = [
        CrystalLine(1, "directory", "name", 1, DiffractionType.success, "comment", ""),
        CrystalLine(
            2, "directory2", "name2", 1, DiffractionType.success, "comment2", ""
        ),
    ]
    crystals = [
        # This crystal matches the first line of the spreadsheet, so it will be renamed from "directory"
        # to "name".
        P11Crystal(
            "directory",
            runs=[
                P11Run(
                    1,
                    Path(__file__).parent,
                    parse_p11_info_file(
                        Path(__file__).parent / "info.txt", UnitRegistry()
                    ),
                    data_raw_filename_pattern=None,
                    microscope_image_filename_pattern=None,
                )
            ],
        ),
    ]
    new_crystals, has_warnings = process_and_validate_with_spreadsheet(
        spreadsheet_lines, crystals
    )
    assert len(new_crystals) == 1
    assert new_crystals[0].crystal_id == "name"
    assert has_warnings


def test_process_and_validate_with_spreadsheet_duplicate_lines() -> None:
    # Here we have two lines that are exactly the same w.r.t. the primary keys
    spreadsheet_lines = [
        CrystalLine(1, "directory", "name", 1, DiffractionType.success, "comment", ""),
        # Different directory, same crystal, same run ID (this simulates a typo)
        CrystalLine(
            2, "directory2", "name", 1, DiffractionType.success, "comment2", ""
        ),
    ]
    new_crystals, has_warnings = process_and_validate_with_spreadsheet(
        spreadsheet_lines, []
    )
    assert len(new_crystals) == 0
    assert has_warnings


def test_process_and_validate_with_spreadsheet_different_name_for_same_run() -> None:
    # This is tricky: we want to update existing crystals with the new name given in the spreadsheet.
    # But what if we have two different names for two different runs for the same crystal?
    spreadsheet_lines = [
        CrystalLine(1, "directory", "name", 1, DiffractionType.success, "", ""),
        CrystalLine(
            2, "directory", "differentname", 2, DiffractionType.success, "", ""
        ),
    ]
    new_crystals, has_warnings = process_and_validate_with_spreadsheet(
        spreadsheet_lines,
        [
            P11Crystal(
                "directory",
                runs=[
                    P11Run(
                        1,
                        Path(__file__).parent,
                        parse_p11_info_file(
                            Path(__file__).parent / "info.txt", UnitRegistry()
                        ),
                        data_raw_filename_pattern=None,
                        microscope_image_filename_pattern=None,
                    ),
                    P11Run(
                        2,
                        Path(__file__).parent,
                        parse_p11_info_file(
                            Path(__file__).parent / "info.txt", UnitRegistry()
                        ),
                        data_raw_filename_pattern=None,
                        microscope_image_filename_pattern=None,
                    ),
                ],
            )
        ],
    )
    # We still have a crystal with a single run. Might be a bad choice.
    assert len(new_crystals) == 0
    assert has_warnings
