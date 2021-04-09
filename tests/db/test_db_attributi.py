import pytest

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoTypeInt
from amarcord.db.constants import MANUAL_SOURCE_NAME
from amarcord.db.db import DB
from amarcord.db.proposal_id import ProposalId
from amarcord.db.raw_attributi_map import RawAttributiMap
from amarcord.db.table_classes import DBSample

SAMPLE_NAME = "sample_name"

RUN_ID = 1

ATTRIBUTO_VALUE = 3

NEW_ATTRIBUTO_VALUE = 5

PROPOSAL_ID = ProposalId(1)


@pytest.mark.parametrize("name", ["foo-bar", "0ad"])
def test_add_invalid_name(db: DB, name: str) -> None:
    """ Check if we cannot add certain attributi names """
    with pytest.raises(ValueError):
        with db.connect() as conn:
            db.add_attributo(
                conn,
                name,
                "description",
                AssociatedTable.SAMPLE,
                AttributoTypeInt(),
            )


def test_add_and_retrieve_attributo(db: DB) -> None:
    """ Check if we cannot add and then retrieve an attributo for all possible tables """
    for table in (AssociatedTable.RUN, AssociatedTable.SAMPLE):
        aid = f"{table.value}_attributo"
        with db.connect() as conn:
            db.add_attributo(
                conn,
                aid,
                "description",
                table,
                AttributoTypeInt(),
            )

            attributi = db.retrieve_table_attributi(conn, table)

            attributo = attributi.get(AttributoId(aid), None)

            assert attributo is not None
            assert attributo.name == aid
            assert attributo.description == "description"
            assert attributo.associated_table == table
            assert isinstance(attributo.attributo_type, AttributoTypeInt)


def test_add_set_delete_run_attributo(db: DB) -> None:
    """ Check if we cannot add and then assign an attributo to a run """
    table = AssociatedTable.RUN
    aid = f"{table.value}_attributo"
    with db.connect() as conn:
        db.add_proposal(conn, PROPOSAL_ID)

        # First, try adding a run with a non-existing attributo
        with pytest.raises(ValueError):
            db.add_run(
                conn,
                PROPOSAL_ID,
                run_id=RUN_ID,
                sample_id=None,
                attributi=RawAttributiMap({MANUAL_SOURCE_NAME: {aid: ATTRIBUTO_VALUE}}),
            )

        # Then add the attributo and a run with it
        db.add_attributo(
            conn,
            aid,
            "description",
            table,
            AttributoTypeInt(),
        )

        db.add_run(
            conn,
            PROPOSAL_ID,
            run_id=RUN_ID,
            sample_id=None,
            attributi=RawAttributiMap({MANUAL_SOURCE_NAME: {aid: ATTRIBUTO_VALUE}}),
        )

        # Retrieve the run, check if the attributo is there and has the correct value
        run = db.retrieve_run(conn, RUN_ID)

        assert run.attributi.select_int(AttributoId(aid)) == ATTRIBUTO_VALUE

        # Then delete the attributo, re-retrieve run, check that attributo is not there.
        db.delete_attributo(conn, table, AttributoId(aid))

        run = db.retrieve_run(conn, RUN_ID)
        assert run.attributi.select_int(AttributoId(aid)) is None


def test_add_set_delete_sample_attributo(db: DB) -> None:
    """ Check if we cannot add and then assign an attributo to a sample """
    table = AssociatedTable.SAMPLE
    aid = f"{table.value}_attributo"
    with db.connect() as conn:
        db.add_proposal(conn, PROPOSAL_ID)

        # First, try adding a sample with a non-existing attributo
        with pytest.raises(ValueError):
            db.add_sample(
                conn,
                DBSample(
                    id=None,
                    name=SAMPLE_NAME,
                    attributi=RawAttributiMap(
                        {MANUAL_SOURCE_NAME: {aid: ATTRIBUTO_VALUE}}
                    ),
                ),
            )

        # Then add the attributo and a sample with it
        db.add_attributo(
            conn,
            aid,
            "description",
            table,
            AttributoTypeInt(),
        )

        db.add_sample(
            conn,
            DBSample(
                id=None,
                name=SAMPLE_NAME,
                attributi=RawAttributiMap({MANUAL_SOURCE_NAME: {aid: ATTRIBUTO_VALUE}}),
            ),
        )

        # Retrieve the sample, check if the attributo is there and has the correct value
        samples = db.retrieve_samples(conn)

        assert len(samples) == 1

        sample = samples[0]

        assert sample.attributi.select_int(AttributoId(aid)) == ATTRIBUTO_VALUE

        # Then delete the attributo, re-retrieve sample, check that attributo is not there.
        db.delete_attributo(conn, AssociatedTable.SAMPLE, AttributoId(aid))

        samples = db.retrieve_samples(conn)

        assert len(samples) == 1

        sample = samples[0]

        assert sample.attributi.select_int(AttributoId(aid)) is None


def test_update_run_attributo(db: DB) -> None:
    """ Check if we can update a run attributo """
    table = AssociatedTable.RUN
    aid = f"{table.value}_attributo"
    with db.connect() as conn:
        db.add_proposal(conn, PROPOSAL_ID)

        # Then add the attributo and a run with it
        db.add_attributo(
            conn,
            aid,
            "description",
            table,
            AttributoTypeInt(),
        )

        db.add_run(
            conn,
            PROPOSAL_ID,
            run_id=RUN_ID,
            sample_id=None,
            attributi=RawAttributiMap({MANUAL_SOURCE_NAME: {aid: ATTRIBUTO_VALUE}}),
        )

        # Now change the attributo and retrieve the run
        db.update_run_attributo(conn, RUN_ID, AttributoId(aid), NEW_ATTRIBUTO_VALUE)

        assert (
            db.retrieve_run(conn, RUN_ID).attributi.select_int(AttributoId(aid))
            == NEW_ATTRIBUTO_VALUE
        )
