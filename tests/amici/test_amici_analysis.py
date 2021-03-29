import logging
from dataclasses import replace
from pathlib import Path

from amarcord.amici.analysis import DeepComparisonResult
from amarcord.amici.analysis import cheetah_to_database
from amarcord.amici.analysis import deep_compare_data_source
from amarcord.amici.analysis import ingest_cheetah_internal
from amarcord.db.db import DB
from amarcord.db.proposal_id import ProposalId
from amarcord.db.raw_attributi_map import RawAttributiMap
from amarcord.db.table_classes import DBDataSource
from amarcord.db.table_classes import DBHitFindingParameters
from amarcord.db.table_classes import DBHitFindingResult
from amarcord.db.table_classes import DBPeakSearchParameters
from amarcord.db.table_classes import DBSample

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.DEBUG
)


def test_cheetah_to_database() -> None:
    data_sources = list(
        cheetah_to_database(
            Path(__file__).parent.parent / "cheetah" / "gui" / "crawler.config"
        )
    )

    assert len(data_sources) == 2
    assert len(data_sources[0].hit_finding_results) == 1
    assert len(data_sources[0].hit_finding_results[0].indexing_results) == 0


def test_deep_compare_datasource_shallow() -> None:
    left = DBDataSource(
        id=None,
        run_id=1,
        number_of_frames=1,
        hit_finding_results=[],
        source=None,
        tag=None,
        comment=None,
    )
    right = replace(left, number_of_frames=2)

    # Simple compare (shallow) works
    assert (
        deep_compare_data_source(left, right)
        == DeepComparisonResult.DATA_SOURCE_DIFFERS
    )
    # Comparing doesn't care about IDs
    assert (
        deep_compare_data_source(left, replace(left, id=1))
        == DeepComparisonResult.NO_DIFFERENCE
    )


def test_deep_compare_datasource_deep() -> None:
    psp = DBPeakSearchParameters(
        id=None, method="method1", software="software1", command_line="cl"
    )
    hfp = DBHitFindingParameters(id=None, min_peaks=1, tag=None, comment=None)
    left_hfr = DBHitFindingResult(
        id=None,
        data_source_id=None,
        peak_search_parameters=psp,
        hit_finding_parameters=hfp,
        result_filename="a",
        number_of_hits=1,
        hit_rate=0.2,
        indexing_results=[],
        tag=None,
        comment=None,
    )
    right_hfr = replace(left_hfr, result_filename="b")
    left = DBDataSource(
        id=None,
        run_id=1,
        number_of_frames=1,
        hit_finding_results=[left_hfr],
        source=None,
        tag=None,
        comment=None,
    )
    right = replace(left, hit_finding_results=[right_hfr])

    # Simple compare (shallow) works
    assert (
        deep_compare_data_source(left, right)
        == DeepComparisonResult.HIT_FINDING_DIFFERS
    )


def test_ingest_cheetah_idempotent(db: DB) -> None:
    with db.connect() as conn:
        db.add_proposal(conn, ProposalId(1))
        sample_id = db.add_sample(
            conn, DBSample(id=None, name="sample1", attributi=RawAttributiMap({}))
        )
        db.add_run(
            conn,
            ProposalId(1),
            run_id=9,
            sample_id=sample_id,
            attributi=RawAttributiMap({}),
        )
        db.add_run(
            conn,
            ProposalId(1),
            run_id=13,
            sample_id=sample_id,
            attributi=RawAttributiMap({}),
        )
        first_ingest = ingest_cheetah_internal(
            Path(__file__).parent.parent / "cheetah" / "gui" / "crawler.config",
            db,
            conn,
        )
        assert first_ingest.number_of_ingested_data_sources == 2
        analysis_results = db.retrieve_sample_based_analysis(conn)
        assert len(analysis_results) == 1
        assert len(analysis_results[0].indexing_paths) == 2
        assert analysis_results[0].indexing_paths[0].hit_finding_results

        second_ingest = ingest_cheetah_internal(
            Path(__file__).parent.parent / "cheetah" / "gui" / "crawler.config",
            db,
            conn,
        )
        assert second_ingest.number_of_ingested_data_sources == 0


def test_ingest_cheetah_dont_create_new_data_source(db: DB) -> None:
    with db.connect() as conn:
        db.add_proposal(conn, ProposalId(1))
        sample_id = db.add_sample(
            conn, DBSample(id=None, name="sample1", attributi=RawAttributiMap({}))
        )
        db.add_run(
            conn,
            ProposalId(1),
            run_id=9,
            sample_id=sample_id,
            attributi=RawAttributiMap({}),
        )
        db.add_run(
            conn,
            ProposalId(1),
            run_id=13,
            sample_id=sample_id,
            attributi=RawAttributiMap({}),
        )
        first_ingest = ingest_cheetah_internal(
            Path(__file__).parent.parent / "cheetah" / "gui" / "crawler.config",
            db,
            conn,
        )
        assert first_ingest.number_of_ingested_data_sources == 2

        # The updated directory is the same as the normal one, but has a different "min SNR" value.
        # Doesn't matter which value you change, as long as the data source (i.e. the source files) stay the same.
        second_ingest = ingest_cheetah_internal(
            Path(__file__).parent.parent / "cheetah-update" / "gui" / "crawler.config",
            db,
            conn,
        )
        assert second_ingest.number_of_ingested_data_sources == 2
