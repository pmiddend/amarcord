import datetime
from random import choice
from random import randint
from random import randrange
from random import seed
from typing import List

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoTypeDateTime
from amarcord.db.attributo_type import AttributoTypeDouble
from amarcord.db.attributo_type import AttributoTypeDuration
from amarcord.db.attributo_type import AttributoTypeInt
from amarcord.db.attributo_type import AttributoTypeList
from amarcord.db.attributo_type import AttributoTypeString
from amarcord.db.attributo_type import AttributoTypeTags
from amarcord.db.attributo_type import AttributoTypeUserName
from amarcord.db.db import Connection
from amarcord.db.db import DB
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.proposal_id import ProposalId
from amarcord.db.raw_attributi_map import RawAttributiMap
from amarcord.db.table_classes import DBSample
from amarcord.db.tables import logger
from amarcord.numeric_range import NumericRange


def create_sample_data(db: DB) -> None:
    logger.info("Creating sample data...")
    with db.connect() as conn:
        db.add_event(
            conn,
            EventLogLevel.INFO,
            source="karabo",
            text="test line please ignore",
            created=None,
        )
        proposal_id = 1

        # Create proposal
        db.add_proposal(conn, ProposalId(proposal_id), "foobar")

        # Second proposal in case you want to test the proposal chooser
        # conn.execute(
        #     tables.proposal.insert().values(
        #         id=2, metadata={"data": {}, "title": "shit proposal"}
        #     )
        # )

        add_xfel_2696_attributi(db, conn)

        # Create samples
        sample_ids: List[int] = []
        for i in range(10):
            attributi = RawAttributiMap({})
            attributi.set_single_manual(
                AttributoId("crystal_buffer"), "foo crystal buffer bar"
            )
            attributi.set_single_manual(AttributoId("creator"), "pmidden")
            attributi.set_single_manual(AttributoId("shaking_time"), "P2D")
            sample_ids.append(
                db.add_sample(
                    conn,
                    DBSample(
                        id=None,
                        proposal_id=ProposalId(proposal_id),
                        name=f"mpro {i}",
                        attributi=attributi,
                    ),
                )
            )

        db.add_sample(
            conn,
            DBSample(
                id=None,
                proposal_id=ProposalId(proposal_id),
                name="mpro unused",
                attributi=RawAttributiMap({}),
            ),
        )

        db.add_attributo(
            conn,
            "repetition_rate",
            description="Repetition Rate",
            associated_table=AssociatedTable.RUN,
            prop_type=AttributoTypeList(
                sub_type=AttributoTypeInt(),
                min_length=None,
                max_length=None,
            ),
        )
        db.add_attributo(
            conn,
            "status",
            description="Status",
            associated_table=AssociatedTable.RUN,
            prop_type=AttributoTypeString(),
        )
        db.add_attributo(
            conn,
            "first_train",
            description="First Train",
            associated_table=AssociatedTable.RUN,
            prop_type=AttributoTypeInt(),
        )
        db.add_attributo(
            conn,
            "last_train",
            description="Last Train",
            associated_table=AssociatedTable.RUN,
            prop_type=AttributoTypeInt(),
        )
        db.add_attributo(
            conn,
            "tags",
            description="Tags",
            associated_table=AssociatedTable.RUN,
            prop_type=AttributoTypeTags(),
        )
        db.add_attributo(
            conn,
            name="started",
            description="Started",
            associated_table=AssociatedTable.RUN,
            prop_type=AttributoTypeDateTime(),
        )

        # Create runs
        _base_date = datetime.datetime.utcnow()
        # To always get the same sample data, yet somewhat random values
        seed(1337)
        run_ids: List[int] = []
        current_train = 0
        for run_id in range(1, 100):
            train_count = randint(600, 3000)
            attributi = RawAttributiMap({})
            attributi.append_to_source(
                "online",
                {
                    AttributoId("status"): "running_online",
                    AttributoId("repetition_rate"): [randrange(0, 20)],
                    AttributoId("first_train"): current_train + 1,
                    AttributoId("last_train"): current_train + train_count,
                },
            )
            attributi.append_to_source(
                "offline",
                {
                    AttributoId("status"): "running_offline",
                    AttributoId("repetition_rate"): [randrange(0, 20)],
                    AttributoId("first_train"): current_train * 10000 + 1,
                    AttributoId("last_train"): current_train * 10000 + train_count,
                },
            )
            # If we want manual attributi as well
            # attributi.append_to_source(
            #     "manual",
            #     {
            #         AttributoId("status"): "running_manual",
            #         AttributoId("repetition_rate"): [randrange(0, 20)],
            #         AttributoId("first_train"): current_train * (-10000) + 1,
            #         AttributoId("last_train"): current_train * (-10000) + train_count,
            #     },
            # )
            db.add_run(
                conn,
                ProposalId(proposal_id),
                run_id=run_id,
                sample_id=choice(sample_ids),
                attributi=attributi,
            )

            current_train += train_count

            run_ids.append(run_id)

            for _ in range(randrange(0, 10)):
                db.add_comment(
                    conn,
                    run_id,
                    "testauthor",
                    choice(
                        [
                            "this is unstable",
                            "we changed the nozzles here",
                            "automatic message foo bar",
                        ]
                    ),
                )

    logger.info("Done")


def add_xfel_2696_attributi(db: DB, conn: Connection) -> None:
    db.add_attributo(
        conn,
        name="crystal_buffer",
        description="Crystal Buffer",
        associated_table=AssociatedTable.SAMPLE,
        prop_type=AttributoTypeString(),
    )
    db.add_attributo(
        conn,
        name="shaking_time",
        description="Shaking Time",
        associated_table=AssociatedTable.SAMPLE,
        prop_type=AttributoTypeDuration(),
    )
    db.add_attributo(
        conn,
        name="avg_crystal_size",
        description="Average Crystal Size",
        associated_table=AssociatedTable.SAMPLE,
        prop_type=AttributoTypeDouble(
            range=NumericRange(
                0, minimum_inclusive=False, maximum=None, maximum_inclusive=False
            ),
            suffix="μm",
            standard_unit=True,
        ),
    )
    db.add_attributo(
        conn,
        name="crystallization_temperature",
        description="Crystallization Temperature",
        associated_table=AssociatedTable.SAMPLE,
        prop_type=AttributoTypeDouble(range=None, suffix="°C", standard_unit=False),
    )
    db.add_attributo(
        conn,
        name="shaking_strength",
        description="Shaking Strength",
        associated_table=AssociatedTable.SAMPLE,
        prop_type=AttributoTypeDouble(
            range=NumericRange(
                0, minimum_inclusive=True, maximum=None, maximum_inclusive=False
            ),
            suffix="RPM",
            standard_unit=False,
        ),
    )
    db.add_attributo(
        conn,
        name="protein_concentration",
        description="Protein Concentration",
        associated_table=AssociatedTable.SAMPLE,
        prop_type=AttributoTypeDouble(
            range=NumericRange(
                0, minimum_inclusive=True, maximum=None, maximum_inclusive=False
            ),
            suffix="mg/mL",
            standard_unit=True,
        ),
    )
    db.add_attributo(
        conn,
        name="comment",
        description="Comment",
        associated_table=AssociatedTable.SAMPLE,
        prop_type=AttributoTypeString(),
    )
    db.add_attributo(
        conn,
        name="crystal_settlement_volume",
        description="Crystal Settlement Volume",
        associated_table=AssociatedTable.SAMPLE,
        prop_type=AttributoTypeDouble(
            range=NumericRange(
                0, minimum_inclusive=True, maximum=100, maximum_inclusive=True
            ),
            suffix="%",
            standard_unit=False,
        ),
    )
    db.add_attributo(
        conn,
        name="seed_stock_used",
        description="Seed Stock Used",
        associated_table=AssociatedTable.SAMPLE,
        prop_type=AttributoTypeString(),
    )
    db.add_attributo(
        conn,
        name="plate_origin",
        description="Plate Origin",
        associated_table=AssociatedTable.SAMPLE,
        prop_type=AttributoTypeString(),
    )
    db.add_attributo(
        conn,
        name="creator",
        description="Creator",
        associated_table=AssociatedTable.SAMPLE,
        prop_type=AttributoTypeUserName(),
    )
    db.add_attributo(
        conn,
        name="crystallization_method",
        description="Crystallization Method",
        associated_table=AssociatedTable.SAMPLE,
        prop_type=AttributoTypeString(),
    )
    db.add_attributo(
        conn,
        name="incubation_time",
        description="Incubation Time",
        associated_table=AssociatedTable.SAMPLE,
        prop_type=AttributoTypeDateTime(),
    )
    db.add_attributo(
        conn,
        name="crystal_shape",
        description="Crystal Shape",
        associated_table=AssociatedTable.SAMPLE,
        prop_type=AttributoTypeList(
            sub_type=AttributoTypeDouble(
                range=NumericRange(
                    0, minimum_inclusive=True, maximum=None, maximum_inclusive=True
                ),
                suffix="nm",
                standard_unit=True,
            ),
            min_length=3,
            max_length=3,
        ),
    )
    db.add_attributo(
        conn,
        name="filters",
        description="Filters",
        associated_table=AssociatedTable.SAMPLE,
        prop_type=AttributoTypeList(
            sub_type=AttributoTypeString(), min_length=None, max_length=None
        ),
    )
