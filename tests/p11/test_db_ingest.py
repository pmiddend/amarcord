from dataclasses import replace
from pathlib import Path

import sqlalchemy as sa
from pint import UnitRegistry

from amarcord.amici.p11.analyze_filesystem import P11Crystal
from amarcord.amici.p11.analyze_filesystem import P11Run
from amarcord.amici.p11.db import DiffractionType
from amarcord.amici.p11.db import table_crystals
from amarcord.amici.p11.db import table_data_reduction
from amarcord.amici.p11.db import table_diffractions
from amarcord.amici.p11.db import table_pucks
from amarcord.amici.p11.db_ingest import EIGER_16_M_DETECTOR_NAME
from amarcord.amici.p11.db_ingest import MetadataRetriever
from amarcord.amici.p11.db_ingest import empty_metadata_retriever
from amarcord.amici.p11.db_ingest import ingest_diffractions_for_crystals
from amarcord.amici.p11.db_ingest import ingest_reductions_for_crystals
from amarcord.amici.p11.db_ingest import insert_diffraction
from amarcord.amici.p11.parser import parse_p11_info_file
from amarcord.amici.p11.spreadsheet_reader import CrystalLine
from amarcord.amici.xds.analyze_filesystem import XDSFilesystem
from amarcord.cli.p11_filesystem_ingester import process_and_validate_with_spreadsheet
from amarcord.modules.dbcontext import CreationMode
from amarcord.modules.dbcontext import DBContext

DEFAULT_METADATA_RETRIEVER = MetadataRetriever(
    lambda _crystal_id, _run_id: DiffractionType.success,
    lambda _crystal_id, _run_id: "",
    EIGER_16_M_DETECTOR_NAME,
)


def test_db_ingest_diffractions_successful(db) -> None:
    dbcontext = DBContext("sqlite://", echo=False)
    crystals = table_crystals(dbcontext.metadata, table_pucks(dbcontext.metadata))
    diffs = table_diffractions(dbcontext.metadata, crystals)
    dbcontext.create_all(creation_mode=CreationMode.DONT_CHECK)

    with dbcontext.connect() as conn:
        crystal_id = "c1"
        conn.execute(sa.insert(crystals).values(crystal_id=crystal_id))

        crystal_list = [
            P11Crystal(
                crystal_id,
                runs=[
                    P11Run(
                        1,
                        parse_p11_info_file(
                            Path(__file__).parent / "info.txt", UnitRegistry()
                        ),
                        data_raw_filename_pattern=None,
                        microscope_image_filename_pattern=None,
                        processed_path=None,
                    )
                ],
            )
        ]
        has_warnings = ingest_diffractions_for_crystals(
            conn,
            diffs,
            crystals,
            crystal_list,
            insert_diffraction_if_not_exists=True,
            metadata_retriever=empty_metadata_retriever(EIGER_16_M_DETECTOR_NAME),
        )
        assert not has_warnings

    with dbcontext.connect() as conn:
        diffraction_ids = conn.execute(sa.select([diffs.c.crystal_id])).fetchall()

        assert len(diffraction_ids) == 1
        assert diffraction_ids[0][0] == crystal_id


def test_db_ingest_diffractions_crystal_in_filesystem_but_not_in_db(db) -> None:
    dbcontext = DBContext("sqlite://", echo=False)
    crystals = table_crystals(dbcontext.metadata, table_pucks(dbcontext.metadata))
    diffs = table_diffractions(dbcontext.metadata, crystals)
    dbcontext.create_all(creation_mode=CreationMode.DONT_CHECK)

    with dbcontext.connect() as conn:
        crystal_id = "c1"

        crystal_list = [
            P11Crystal(
                crystal_id,
                runs=[
                    P11Run(
                        1,
                        parse_p11_info_file(
                            Path(__file__).parent / "info.txt", UnitRegistry()
                        ),
                        data_raw_filename_pattern=None,
                        microscope_image_filename_pattern=None,
                        processed_path=None,
                    )
                ],
            )
        ]
        has_warnings = ingest_diffractions_for_crystals(
            conn,
            diffs,
            crystals,
            crystal_list,
            insert_diffraction_if_not_exists=True,
            metadata_retriever=empty_metadata_retriever(EIGER_16_M_DETECTOR_NAME),
        )
        assert has_warnings

    with dbcontext.connect() as conn:
        diffraction_ids = conn.execute(sa.select([diffs.c.crystal_id])).fetchall()

        assert not diffraction_ids


def test_db_ingest_diffractions_diffraction_does_not_exist_and_not_add_it(db) -> None:
    dbcontext = DBContext("sqlite://", echo=False)
    crystals = table_crystals(dbcontext.metadata, table_pucks(dbcontext.metadata))
    diffs = table_diffractions(dbcontext.metadata, crystals)
    dbcontext.create_all(creation_mode=CreationMode.DONT_CHECK)

    with dbcontext.connect() as conn:
        crystal_id = "c1"
        conn.execute(sa.insert(crystals).values(crystal_id=crystal_id))

        crystal_list = [
            P11Crystal(
                crystal_id,
                runs=[
                    P11Run(
                        1,
                        parse_p11_info_file(
                            Path(__file__).parent / "info.txt", UnitRegistry()
                        ),
                        data_raw_filename_pattern=None,
                        microscope_image_filename_pattern=None,
                        processed_path=None,
                    )
                ],
            )
        ]
        has_warnings = ingest_diffractions_for_crystals(
            conn,
            diffs,
            crystals,
            crystal_list,
            insert_diffraction_if_not_exists=False,
            metadata_retriever=empty_metadata_retriever(EIGER_16_M_DETECTOR_NAME),
        )
        assert not has_warnings

    with dbcontext.connect() as conn:
        diffraction_ids = conn.execute(sa.select([diffs.c.crystal_id])).fetchall()

        assert not diffraction_ids


def test_db_ingest_diffractions_update_diffraction_if_exists(db) -> None:
    dbcontext = DBContext("sqlite://", echo=False)
    crystals = table_crystals(dbcontext.metadata, table_pucks(dbcontext.metadata))
    diffs = table_diffractions(dbcontext.metadata, crystals)
    dbcontext.create_all(creation_mode=CreationMode.DONT_CHECK)

    with dbcontext.connect() as conn:
        crystal_id = "c1"
        conn.execute(sa.insert(crystals).values(crystal_id=crystal_id))

        info_file = parse_p11_info_file(
            Path(__file__).parent / "info.txt", UnitRegistry()
        )
        run = P11Run(
            1,
            info_file,
            data_raw_filename_pattern=None,
            microscope_image_filename_pattern=None,
            processed_path=None,
        )
        # "Pre-insert" the diffraction, using a different number of frames as an example field
        insert_diffraction(
            conn,
            crystal_id,
            diffs,
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
            diffs,
            crystals,
            crystal_list,
            insert_diffraction_if_not_exists=False,
            metadata_retriever=empty_metadata_retriever(EIGER_16_M_DETECTOR_NAME),
        )
        assert not has_warnings

    with dbcontext.connect() as conn:
        frames = conn.execute(sa.select([diffs.c.number_of_frames])).fetchone()

        # We have pre-inserted "frames+1" and now expected "frames" as the updated value
        assert frames[0] == run.info_file.frames


def test_process_and_validate_with_spreadsheet_run_doesnt_match() -> None:
    spreadsheet_lines = [
        CrystalLine(1, "directory", "name", 1, DiffractionType.success, "comment"),
        CrystalLine(2, "directory2", "name2", 1, DiffractionType.success, "comment2"),
    ]
    crystals = [
        # This crystal matches the first line of the spreadsheet, so it will be renamed from "directory"
        # to "name"
        P11Crystal(
            "directory",
            runs=[
                P11Run(
                    1,
                    parse_p11_info_file(
                        Path(__file__).parent / "info.txt", UnitRegistry()
                    ),
                    data_raw_filename_pattern=None,
                    microscope_image_filename_pattern=None,
                    processed_path=None,
                )
            ],
        ),
        # This crystals matches, too, but not the run number, so it will cause a warning
        P11Crystal(
            "name",
            runs=[
                P11Run(
                    2,
                    parse_p11_info_file(
                        Path(__file__).parent / "info.txt", UnitRegistry()
                    ),
                    data_raw_filename_pattern=None,
                    microscope_image_filename_pattern=None,
                    processed_path=None,
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
        CrystalLine(1, "directory", "name", 1, DiffractionType.success, "comment"),
        CrystalLine(2, "directory2", "name2", 1, DiffractionType.success, "comment2"),
    ]
    crystals = [
        # This crystal matches the first line of the spreadsheet, so it will be renamed from "directory"
        # to "name".
        P11Crystal(
            "directory",
            runs=[
                P11Run(
                    1,
                    parse_p11_info_file(
                        Path(__file__).parent / "info.txt", UnitRegistry()
                    ),
                    data_raw_filename_pattern=None,
                    microscope_image_filename_pattern=None,
                    processed_path=None,
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
        CrystalLine(1, "directory", "name", 1, DiffractionType.success, "comment"),
        # Different directory, same crystal, same run ID (this simulates a typo)
        CrystalLine(2, "directory2", "name", 1, DiffractionType.success, "comment2"),
    ]
    new_crystals, has_warnings = process_and_validate_with_spreadsheet(
        spreadsheet_lines, []
    )
    assert len(new_crystals) == 0
    assert has_warnings


def test_ingest_reduction_without_diffraction(db) -> None:
    # We might want to ingest a reduction, but don't have a diffraction (because it wasn't part of the original
    # beamtime, for example). In this case, we shouldn't crash, but ignore the entry

    dbcontext = DBContext("sqlite://", echo=False)
    table_crystals_ = table_crystals(
        dbcontext.metadata, table_pucks(dbcontext.metadata)
    )
    diffs = table_diffractions(dbcontext.metadata, table_crystals_)
    reductions = table_data_reduction(dbcontext.metadata, table_crystals_)
    dbcontext.create_all(creation_mode=CreationMode.DONT_CHECK)

    with dbcontext.connect() as conn:
        processed_path = Path("/")
        crystals = [
            P11Crystal(
                "crystal_id",
                runs=[
                    P11Run(
                        1,
                        parse_p11_info_file(
                            Path(__file__).parent / "info.txt", UnitRegistry()
                        ),
                        data_raw_filename_pattern=None,
                        microscope_image_filename_pattern=None,
                        processed_path=processed_path,
                    )
                ],
            )
        ]
        has_warnings = ingest_reductions_for_crystals(
            conn,
            reductions,
            diffs,
            crystals,
            {
                # this is invalid, but shouldn't matter, we are testing if we are _considering_ this reduction even
                processed_path: XDSFilesystem(
                    correct_lp=None,  # type: ignore
                    results_file=None,  # type: ignore
                    mtz_file=None,
                    analysis_time=None,  # type: ignore
                )
            },
        )
        assert not has_warnings


def test_process_and_validate_with_spreadsheet_different_name_for_same_run() -> None:
    # This is tricky: we want to update existing crystals with the new name given in the spreadsheet.
    # But what if we have two different names for two different runs for the same crystal?
    spreadsheet_lines = [
        CrystalLine(1, "directory", "name", 1, DiffractionType.success, ""),
        CrystalLine(2, "directory", "differentname", 2, DiffractionType.success, ""),
    ]
    new_crystals, has_warnings = process_and_validate_with_spreadsheet(
        spreadsheet_lines,
        [
            P11Crystal(
                "directory",
                runs=[
                    P11Run(
                        1,
                        parse_p11_info_file(
                            Path(__file__).parent / "info.txt", UnitRegistry()
                        ),
                        data_raw_filename_pattern=None,
                        microscope_image_filename_pattern=None,
                        processed_path=None,
                    ),
                    P11Run(
                        2,
                        parse_p11_info_file(
                            Path(__file__).parent / "info.txt", UnitRegistry()
                        ),
                        data_raw_filename_pattern=None,
                        microscope_image_filename_pattern=None,
                        processed_path=None,
                    ),
                ],
            )
        ],
    )
    # We still have a crystal with a single run. Might be a bad choice.
    assert len(new_crystals) == 0
    assert has_warnings
