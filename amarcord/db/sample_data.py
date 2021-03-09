import datetime
from random import randint, random, randrange, seed
from typing import Optional

from amarcord.db.constants import MANUAL_SOURCE_NAME
from amarcord.db.tables import DBTables, logger
from amarcord.db.associated_table import AssociatedTable
from amarcord.modules.dbcontext import DBContext


def create_sample_data(context: DBContext, tables: DBTables) -> None:
    logger.info("Creating sample data...")
    karabo_data: Optional[bytes]
    try:
        with open("data/pickled_karabo", "rb") as f:
            karabo_data = f.read()
    except:
        karabo_data = None

    with context.connect() as conn:
        proposal_id = 1
        # Create proposal
        conn.execute(
            tables.proposal.insert().values(
                id=proposal_id, metadata={"data": {}, "title": "test proposal"}
            )
        )
        # Second proposal in case you want to test the proposal chooser
        # conn.execute(
        #     tables.proposal.insert().values(
        #         id=2, metadata={"data": {}, "title": "shit proposal"}
        #     )
        # )
        # Create targets
        first_target_id = conn.execute(
            tables.target.insert().values(name="Main Protease", short_name="MPro")
        ).inserted_primary_key[0]
        # pylint: disable=unused-variable
        second_target_id = conn.execute(
            tables.target.insert().values(
                name="Protein Like Protease", short_name="PlPro"
            )
        ).inserted_primary_key[0]

        # Sample attributi
        conn.execute(
            tables.attributo.insert().values(
                [
                    {
                        "name": "crystal_buffer",
                        "description": "Crystal Buffer",
                        "json_schema": {"type": "string"},
                        "associated_table": AssociatedTable.SAMPLE,
                    },
                ]
            )
        )

        # Create samples
        first_sample_result = conn.execute(
            tables.sample.insert().values(
                target_id=first_target_id,
                average_crystal_size=1.0,
                crystal_shape=[3.0, 4.0, 5.0],
                modified=datetime.datetime.utcnow(),
                attributi={
                    MANUAL_SOURCE_NAME: {"crystal_buffer": "foo crystal buffer bar"}
                },
            )
        )
        conn.execute(
            tables.sample.insert().values(
                target_id=first_target_id,
                average_crystal_size=2.0,
                modified=datetime.datetime.utcnow(),
                attributi={},
            )
        )
        first_sample_id = first_sample_result.inserted_primary_key[0]

        # Create run properties
        conn.execute(
            tables.attributo.insert().values(
                [
                    {
                        "name": "repetition_rate",
                        "description": "Repetition Rate",
                        "suffix": "MHz",
                        "json_schema": {"type": "number"},
                        "associated_table": AssociatedTable.RUN,
                    },
                    {
                        "name": "status",
                        "description": "Status",
                        "suffix": None,
                        "json_schema": {
                            "type": "string",
                            "enum": ["running", "finished"],
                        },
                        "associated_table": AssociatedTable.RUN,
                    },
                    {
                        "name": "hit_rate",
                        "description": "Hit Rate",
                        "suffix": "%",
                        "json_schema": {
                            "type": "number",
                            "minimum": 0,
                            "maximum": 100,
                        },
                        "associated_table": AssociatedTable.RUN,
                    },
                    {
                        "name": "trains",
                        "description": "Train count",
                        "suffix": None,
                        "json_schema": {"type": "integer"},
                        "associated_table": AssociatedTable.RUN,
                    },
                    {
                        "name": "injector_position_z",
                        "description": "Injector Position Z",
                        "suffix": "mm",
                        "json_schema": {"type": "number"},
                        "associated_table": AssociatedTable.RUN,
                    },
                    {
                        "name": "tags",
                        "description": "Tags",
                        "suffix": None,
                        "json_schema": {
                            "type": "array",
                            "items": {"type": "string", "minLength": 1},
                        },
                        "associated_table": AssociatedTable.RUN,
                    },
                    {
                        "name": "started",
                        "description": "Started",
                        "suffix": None,
                        "json_schema": {"type": "string", "format": "date-time"},
                        "associated_table": AssociatedTable.RUN,
                    },
                ]
            )
        )

        # Create runs
        base_date = datetime.datetime.utcnow()
        # To always get the same sample data, yet somewhat random values
        seed(1337)
        for _ in range(20):
            _run_id = conn.execute(
                tables.run.insert().values(
                    proposal_id=proposal_id,
                    modified=datetime.datetime.utcnow(),
                    sample_id=first_sample_id,
                    karabo=karabo_data,
                    attributi={
                        "karabo": {
                            "started": (
                                base_date + datetime.timedelta(0, randint(10, 200))
                            ).isoformat(),
                            "status": "running",
                            "repetition_rate": randrange(0, 20),
                            "hit_rate": random(),
                            "injector_position_z": randrange(0, 100),
                        }
                    },
                )
            ).inserted_primary_key[0]

        # Add comments as well?
        # for _ in range(50):
        #     conn.execute(
        #         tables.run_comment.insert().values(
        #             run_id=run_id,
        #             author="testauthor",
        #             comment_text="foooooo",
        #             created=datetime.datetime.utcnow(),
        #         )
        #     )
    logger.info("Done")
