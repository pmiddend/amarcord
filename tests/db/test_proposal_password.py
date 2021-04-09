from amarcord.db.db import DB
from amarcord.db.proposal_id import ProposalId

ADMIN_PASSWORD_PLAINTEXT = "shit"

PROPOSAL_ID = ProposalId(1)


def test_check_password(db: DB) -> None:
    with db.connect() as conn:
        db.add_proposal(
            conn, PROPOSAL_ID, admin_password_plaintext=ADMIN_PASSWORD_PLAINTEXT
        )
        assert db.check_proposal_password(conn, PROPOSAL_ID, ADMIN_PASSWORD_PLAINTEXT)
        assert not db.check_proposal_password(
            conn, PROPOSAL_ID, ADMIN_PASSWORD_PLAINTEXT + "shit"
        )


def test_change_and_check_password(db: DB) -> None:
    with db.connect() as conn:
        db.add_proposal(
            conn, PROPOSAL_ID, admin_password_plaintext=ADMIN_PASSWORD_PLAINTEXT
        )
        assert db.check_proposal_password(conn, PROPOSAL_ID, ADMIN_PASSWORD_PLAINTEXT)
        db.change_proposal_password(
            conn, PROPOSAL_ID, ADMIN_PASSWORD_PLAINTEXT + "shit"
        )
        assert db.check_proposal_password(
            conn, PROPOSAL_ID, ADMIN_PASSWORD_PLAINTEXT + "shit"
        )
