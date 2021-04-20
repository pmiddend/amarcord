from amarcord.db.alembic import upgrade_to
from amarcord.db.alembic import upgrade_to_head
from amarcord.db.db import DB
from amarcord.db.proposal_id import ProposalId
from amarcord.db.raw_attributi_map import RawAttributiMap
from amarcord.db.table_classes import DBDataSource
from amarcord.db.table_classes import DBHitFindingParameters
from amarcord.db.table_classes import DBHitFindingResult
from amarcord.db.table_classes import DBPeakSearchParameters
from amarcord.db.tables import create_tables
from amarcord.modules.dbcontext import DBContext


def test_cheetah_migration(tmp_path) -> None:
    connection_url = "sqlite:///" + str(tmp_path / "test.db")

    # First, upgrade to the version before the Cheetah migration
    upgrade_to(connection_url, "55ee43aae117")

    dbcontext = DBContext(connection_url)
    db = DB(dbcontext, create_tables(dbcontext))

    with db.connect() as conn:
        db.add_proposal(conn, ProposalId(1))
        run_id = 9
        db.add_run(
            conn,
            ProposalId(1),
            run_id=run_id,
            sample_id=None,
            attributi=RawAttributiMap({}),
        )
        ds_id = db.add_data_source(
            conn, DBDataSource(id=None, run_id=run_id, number_of_frames=0, source={})
        )
        psp_id = db.add_peak_search_parameters(
            conn, DBPeakSearchParameters(id=None, method="dummy", software="dummy")
        )
        hfp_id = db.add_hit_finding_parameters(
            conn,
            DBHitFindingParameters(
                id=None,
                min_peaks=1,
                tag=None,
                comment=None,
                software="",
                software_version="",
            ),
        )
        db.add_hit_finding_result(
            conn,
            DBHitFindingResult(
                id=None,
                peak_search_parameters_id=psp_id,
                hit_finding_parameters_id=hfp_id,
                data_source_id=ds_id,
                result_filename=str(tmp_path / "peaks.txt"),
                peaks_filename=None,
                result_type="",
                average_peaks_event=1,
                average_resolution=2,
                number_of_hits=0,
                hit_rate=0,
            ),
        )

    cxis = [tmp_path / "first.cxi", tmp_path / "second.cxi"]

    for cxi in cxis:
        with cxi.open("w") as f:
            f.write("foo")

    # Then, update to head. The Cheetah migration should run
    upgrade_to_head(connection_url)

    with db.connect() as conn:
        data_sources = db.retrieve_analysis_data_sources(conn)
        hfr = data_sources[0].hit_finding_results[0].hit_finding_result
        assert hfr.peaks_filename == str(tmp_path / "peaks.txt")
        assert hfr.result_filename == (",".join(str(s) for s in cxis))
