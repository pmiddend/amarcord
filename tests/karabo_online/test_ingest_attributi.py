from amarcord.amici.xfel.karabo_action import KaraboRunStart
from amarcord.amici.xfel.karabo_attributo import KaraboAttributo
from amarcord.amici.xfel.karabo_attributo_action import KaraboAttributoAction
from amarcord.amici.xfel.karabo_general import ingest_attributi
from amarcord.amici.xfel.karabo_general import ingest_karabo_action
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoTypeDateTime
from amarcord.db.attributo_type import AttributoTypeInt
from amarcord.db.constants import MANUAL_SOURCE_NAME
from amarcord.db.db import DB
from amarcord.db.proposal_id import ProposalId

NEWIDENTIFIER = "newidentifier"


def test_ingest_single_attributo(db: DB) -> None:
    """
    Here we test if ingesting a simple integer attributo works.
    """
    ingest_attributi(
        db,
        {
            "source": {
                "key": {
                    "group": "dontcare",
                    "attributo": KaraboAttributo(
                        identifier=NEWIDENTIFIER,
                        source="source",
                        key="key",
                        description="description",
                        type_="int",
                        store=True,
                        action_axis=None,
                        filling_value=None,
                        unit="",
                        action=KaraboAttributoAction.COMPUTE_ARITHMETIC_MEAN,
                        value=1,
                        role=None,
                    ),
                }
            }
        },
    )

    with db.connect() as conn:
        attributi = db.run_attributi(conn)
        assert NEWIDENTIFIER in attributi.keys()
        assert isinstance(
            attributi[AttributoId(NEWIDENTIFIER)].attributo_type, AttributoTypeInt
        )


def test_ingest_single_attributo_idempotent(db: DB) -> None:
    """
    Ingesting twice shouldn't recreate the attributo and throw an error or something
    """
    attributi_map = {
        "source": {
            "key": {
                "group": "dontcare",
                "attributo": KaraboAttributo(
                    identifier=NEWIDENTIFIER,
                    source="source",
                    key="key",
                    description="description",
                    type_="int",
                    store=True,
                    action_axis=None,
                    filling_value=None,
                    unit="",
                    action=KaraboAttributoAction.COMPUTE_ARITHMETIC_MEAN,
                    value=1,
                    role=None,
                ),
            }
        }
    }

    # Intentionally called twice here
    # mypy doesn't get the typed dict here
    ingest_attributi(db, attributi_map)  # type: ignore
    ingest_attributi(db, attributi_map)  # type: ignore

    with db.connect() as conn:
        attributi = db.run_attributi(conn)
        assert NEWIDENTIFIER in attributi.keys()
        assert isinstance(
            attributi[AttributoId(NEWIDENTIFIER)].attributo_type, AttributoTypeInt
        )


def test_ingest_attributo_with_store_false(db: DB) -> None:
    """
    The "store" property of an attributo signifies if we just want to check its state or if we really want
    to store it. This we test here by trying to ingest an attributo with store set to false.
    """
    ingest_attributi(
        db,
        {
            "source": {
                "key": {
                    "group": "dontcare",
                    "attributo": KaraboAttributo(
                        identifier=NEWIDENTIFIER,
                        source="source",
                        key="key",
                        description="description",
                        type_="int",
                        store=False,
                        action_axis=None,
                        filling_value=None,
                        unit="",
                        action=KaraboAttributoAction.COMPUTE_ARITHMETIC_MEAN,
                        value=1,
                        role=None,
                    ),
                }
            }
        },
    )

    with db.connect() as conn:
        assert AttributoId(NEWIDENTIFIER) not in db.run_attributi(conn).keys()


def test_ingest_karabo_action_without_attributi(db: DB) -> None:
    with db.connect() as conn:
        db.add_proposal(conn, ProposalId(1))

        ingest_karabo_action(
            KaraboRunStart(
                run_id=1,
                proposal_id=1,
                attributi={},
            ),
            MANUAL_SOURCE_NAME,
            conn,
            db,
            ProposalId(1),
        )

        run = db.retrieve_run(conn, ProposalId(1), 1)
        assert run is not None


def test_ingest_karabo_action_wrong_proposal_id(db: DB) -> None:
    with db.connect() as conn:
        # We are adding proposal 1, but later on get a run with proposal ID 2!
        db.add_proposal(conn, ProposalId(1))

        db.add_attributo(
            conn,
            "timestamp_UTC_initial",
            "",
            AssociatedTable.RUN,
            AttributoTypeDateTime(),
        )

        ingest_karabo_action(
            KaraboRunStart(
                run_id=1,
                # WATCH OUT!
                proposal_id=2,
                attributi={},
            ),
            MANUAL_SOURCE_NAME,
            conn,
            db,
            ProposalId(1),
        )

        assert not db.retrieve_runs(conn, ProposalId(1), since=None)
