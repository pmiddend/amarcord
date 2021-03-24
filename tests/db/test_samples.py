from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoTypeString
from amarcord.db.db import DB
from amarcord.db.db import DBSample
from amarcord.db.raw_attributi_map import RawAttributiMap

MY_ATTRIBUTO = "my_attributo"


def test_add_and_delete_sample(db: DB) -> None:
    with db.connect() as conn:
        assert not db.retrieve_samples(conn)

        new_id = db.add_sample(
            conn,
            DBSample(
                id=None,
                target_id=None,
                compounds=None,
                micrograph=None,
                protocol=None,
                attributi=RawAttributiMap({}),
            ),
        )

        assert new_id is not None

    with db.connect() as conn:
        assert len(db.retrieve_samples(conn)) == 1

    with db.connect() as conn:
        db.delete_sample(conn, new_id)

    with db.connect() as conn:
        assert not db.retrieve_targets(conn)


def test_modify_sample_attributi(db: DB) -> None:
    with db.connect() as conn:
        assert not db.retrieve_samples(conn)

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
                target_id=None,
                compounds=None,
                micrograph=None,
                protocol=None,
                attributi=RawAttributiMap({}),
            ),
        )

        db.update_sample_attributo(conn, new_id, AttributoId(MY_ATTRIBUTO), "foo")

        assert (
            db.retrieve_samples(conn)[0].attributi.select_value(
                AttributoId(MY_ATTRIBUTO)
            )
            == "foo"
        )


def test_remove_attributo(db: DB) -> None:
    with db.connect() as conn:
        assert not db.retrieve_samples(conn)

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
                target_id=None,
                compounds=None,
                micrograph=None,
                protocol=None,
                attributi=RawAttributiMap({}),
            ),
        )

        db.update_sample_attributo(conn, new_id, AttributoId(MY_ATTRIBUTO), None)

        assert (
            db.retrieve_samples(conn)[0].attributi.select_value(
                AttributoId(MY_ATTRIBUTO)
            )
            is None
        )
