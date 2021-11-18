from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoTypeString
from amarcord.db.db import DB
from amarcord.db.proposal_id import ProposalId
from amarcord.db.raw_attributi_map import RawAttributiMap
from amarcord.db.table_classes import DBSample

PROPOSAL_ID = ProposalId(1)

MY_ATTRIBUTO = "my_attributo"


def test_add_and_delete_sample(db: DB) -> None:
    with db.connect() as conn:
        db.add_proposal(conn, PROPOSAL_ID)

        assert not db.retrieve_samples(conn, PROPOSAL_ID)

        new_id = db.add_sample(
            conn,
            DBSample(
                id=None,
                proposal_id=PROPOSAL_ID,
                name="sample1",
                attributi=RawAttributiMap({}),
            ),
        )

        assert new_id is not None

    with db.connect() as conn:
        assert len(db.retrieve_samples(conn, PROPOSAL_ID)) == 1

    with db.connect() as conn:
        db.delete_sample(conn, new_id)


def test_modify_sample_attributi(db: DB) -> None:
    with db.connect() as conn:
        db.add_proposal(conn, PROPOSAL_ID)
        assert not db.retrieve_samples(conn, PROPOSAL_ID)

        db.add_attributo(
            conn,
            MY_ATTRIBUTO,
            "description",
            AssociatedTable.SAMPLE,
            AttributoTypeString(),
        )

        new_id = db.add_sample(
            conn,
            DBSample(
                id=None,
                proposal_id=PROPOSAL_ID,
                name="sample1",
                attributi=RawAttributiMap({}),
            ),
        )

        db.update_sample_attributo(conn, new_id, AttributoId(MY_ATTRIBUTO), "foo")

        assert (
            db.retrieve_samples(conn, PROPOSAL_ID)[0].attributi.select_value(
                AttributoId(MY_ATTRIBUTO)
            )
            == "foo"
        )


def test_remove_attributo(db: DB) -> None:
    with db.connect() as conn:
        db.add_proposal(conn, PROPOSAL_ID)
        assert not db.retrieve_samples(conn, PROPOSAL_ID)

        db.add_attributo(
            conn,
            MY_ATTRIBUTO,
            "description",
            AssociatedTable.SAMPLE,
            AttributoTypeString(),
        )

        new_id = db.add_sample(
            conn,
            DBSample(
                id=None,
                proposal_id=PROPOSAL_ID,
                name="sample1",
                attributi=RawAttributiMap({}),
            ),
        )

        db.update_sample_attributo(conn, new_id, AttributoId(MY_ATTRIBUTO), None)

        assert (
            db.retrieve_samples(conn, PROPOSAL_ID)[0].attributi.select_value(
                AttributoId(MY_ATTRIBUTO)
            )
            is None
        )
