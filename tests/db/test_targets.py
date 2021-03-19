from amarcord.db.db import DB
from amarcord.db.db import DBTarget


def test_add_and_delete_target(db: DB) -> None:
    with db.connect() as conn:
        assert not db.retrieve_targets(conn)

        new_target_id = db.add_target(
            conn,
            DBTarget(
                id=None,
                name="MPro",
                short_name="Main Protease",
                uniprot_id="",
                molecular_weight=None,
            ),
        )

        assert new_target_id is not None

    with db.connect() as conn:
        assert len(db.retrieve_targets(conn)) == 1

    with db.connect() as conn:
        db.delete_target(conn, new_target_id)

    with db.connect() as conn:
        assert not db.retrieve_targets(conn)
