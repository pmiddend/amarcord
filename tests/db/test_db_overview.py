from amarcord.db.associated_table import AssociatedTable
from amarcord.db.db import DB
from amarcord.db.proposal_id import ProposalId
from amarcord.db.raw_attributi_map import RawAttributiMap
from amarcord.db.table_classes import DBSample

PROPOSAL_ID = ProposalId(1)


def test_retrieve_empty_overview(db: DB) -> None:
    with db.connect() as conn:
        assert not db.retrieve_overview(conn, PROPOSAL_ID, db.retrieve_attributi(conn))


def test_single_run(db: DB) -> None:
    """Add a single run and a sample and expect a one-liner as the overview"""
    with db.connect() as conn:
        db.add_proposal(conn, PROPOSAL_ID)

        sample_id = db.add_sample(
            conn,
            DBSample(
                id=None,
                proposal_id=ProposalId(1),
                name="testsample",
                attributi=RawAttributiMap({}),
            ),
        )
        db.add_run(
            conn, PROPOSAL_ID, 1, sample_id=sample_id, attributi=RawAttributiMap({})
        )

        overview = db.retrieve_overview(conn, PROPOSAL_ID, db.retrieve_attributi(conn))
        assert len(overview) == 1

        assert AssociatedTable.RUN in overview[0]
        assert AssociatedTable.SAMPLE in overview[0]


def test_two_runs_one_sample(db: DB) -> None:
    """Add a two runs, one with a sample and one without. Get the overview and expect two according lines."""
    with db.connect() as conn:
        db.add_proposal(conn, PROPOSAL_ID)

        sample_id = db.add_sample(
            conn,
            DBSample(
                id=None,
                proposal_id=ProposalId(1),
                name="testsample",
                attributi=RawAttributiMap({}),
            ),
        )
        db.add_run(conn, PROPOSAL_ID, 1, sample_id, RawAttributiMap({}))
        db.add_run(conn, PROPOSAL_ID, 2, None, RawAttributiMap({}))

        overview = db.retrieve_overview(conn, PROPOSAL_ID, db.retrieve_attributi(conn))
        assert len(overview) == 2
