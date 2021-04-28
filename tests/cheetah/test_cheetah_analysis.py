import logging
from pathlib import Path

from amarcord.amici.cheetah.analysis import cheetah_to_database
from amarcord.amici.cheetah.analysis import ingest_cheetah
from amarcord.db.db import DB
from amarcord.db.proposal_id import ProposalId
from amarcord.db.raw_attributi_map import RawAttributiMap
from amarcord.db.table_classes import DBSample

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.DEBUG
)


def test_cheetah_to_database() -> None:
    """Simply test if converting a cheetah configuration structure converts to our DB structures"""
    parent_path = Path(__file__).parent.parent / "cheetah"
    data_sources = list(cheetah_to_database(parent_path / "gui" / "crawler.config"))

    assert len(data_sources) == 2
    assert len(data_sources[0].hit_finding_results) == 1
    assert data_sources[0].hit_finding_results[
        0
    ].hit_finding_result.result_filename == str(
        parent_path / "hdf5" / "r0213-cry41" / "test.cxi"
    )
    assert len(data_sources[0].hit_finding_results[0].indexing_results) == 0


def test_ingest_cheetah_idempotent(db: DB) -> None:
    """
    We ingest the same data twice with cheetah and expect it to do nothing the second time.
    """
    with db.connect() as conn:
        db.add_proposal(conn, ProposalId(1))
        sample_id = db.add_sample(
            conn,
            DBSample(
                id=None,
                proposal_id=ProposalId(1),
                name="sample1",
                attributi=RawAttributiMap({}),
            ),
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
        first_ingest = ingest_cheetah(
            Path(__file__).parent.parent / "cheetah" / "gui" / "crawler.config",
            db,
            conn,
            ProposalId(1),
            force_run_creation=False,
        )
        assert len(first_ingest.new_data_source_and_run_ids) == 2
        analysis_results = db.retrieve_sample_based_analysis(conn)
        assert len(analysis_results) == 1
        assert len(analysis_results[0].data_sources) == 2
        assert analysis_results[0].data_sources[0].hit_finding_results

        second_ingest = ingest_cheetah(
            Path(__file__).parent.parent / "cheetah" / "gui" / "crawler.config",
            db,
            conn,
            ProposalId(1),
            force_run_creation=False,
        )
        assert not second_ingest.new_data_source_and_run_ids


def test_ingest_cheetah_dont_create_new_data_source(db: DB) -> None:
    with db.connect() as conn:
        db.add_proposal(conn, ProposalId(1))
        sample_id = db.add_sample(
            conn,
            DBSample(
                id=None,
                proposal_id=ProposalId(1),
                name="sample1",
                attributi=RawAttributiMap({}),
            ),
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
        first_ingest = ingest_cheetah(
            Path(__file__).parent.parent / "cheetah" / "gui" / "crawler.config",
            db,
            conn,
            ProposalId(1),
            force_run_creation=False,
        )
        assert len(first_ingest.new_data_source_and_run_ids) == 2

        # The updated directory is the same as the normal one, but has a different "min SNR" value.
        # Doesn't matter which value you change, as long as the data source (i.e. the source files) stay the same.
        second_ingest = ingest_cheetah(
            Path(__file__).parent.parent / "cheetah-update" / "gui" / "crawler.config",
            db,
            conn,
            ProposalId(1),
            force_run_creation=False,
        )
        assert len(second_ingest.new_data_source_and_run_ids) == 2
