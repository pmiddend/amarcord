from dataclasses import replace

import pytest

from amarcord.db.db import Connection
from amarcord.db.db import DB
from amarcord.db.proposal_id import ProposalId
from amarcord.db.raw_attributi_map import RawAttributiMap

PROPOSAL_ID = ProposalId(1)


def add_run(db: DB, conn: Connection) -> int:
    proposal_id = PROPOSAL_ID
    db.add_proposal(conn, proposal_id)
    run_id = 1
    db.add_run(conn, proposal_id, run_id, None, RawAttributiMap({}))
    return run_id


def test_add_comments(db: DB) -> None:
    with db.connect() as conn:
        run_id = add_run(db, conn)

        with pytest.raises(ValueError):
            db.add_comment(conn, run_id, "  ", "foo")

        with pytest.raises(ValueError):
            db.add_comment(conn, run_id, "author", "  ")

        db.add_comment(conn, run_id, "author", "text")
        assert len(db.retrieve_run(conn, PROPOSAL_ID, run_id).comments) == 1

        db.add_comment(conn, run_id, "author2", "text2")
        assert len(db.retrieve_run(conn, PROPOSAL_ID, run_id).comments) == 2


def test_delete_comment(db: DB) -> None:
    with db.connect() as conn:
        run_id = add_run(db, conn)

        comment_id = db.add_comment(conn, run_id, "author", "text")
        db.delete_comment(conn, run_id, comment_id)
        assert not db.retrieve_run(conn, PROPOSAL_ID, run_id).comments


def test_modify_comment(db: DB) -> None:
    with db.connect() as conn:
        run_id = add_run(db, conn)

        db.add_comment(conn, run_id, "author", "text")
        c = replace(
            db.retrieve_run(conn, PROPOSAL_ID, run_id).comments[0], text="modified"
        )
        db.change_comment(conn, c)
        assert db.retrieve_run(conn, PROPOSAL_ID, run_id).comments[0].text == "modified"
