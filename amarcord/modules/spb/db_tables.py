import datetime
from random import randint, random, randrange, seed
from typing import Dict, List, Optional

import logging

import sqlalchemy as sa
from sqlalchemy import func

from amarcord.modules.dbcontext import DBContext
from amarcord.modules.spb.run_property import RunProperty
from amarcord.qt.properties import (
    PropertyInt,
    PropertySample,
    RichPropertyType,
)

logger = logging.getLogger(__name__)


def _table_custom_run_property(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "CustomRunProperty",
        metadata,
        sa.Column("name", sa.String(length=255), primary_key=True),
        sa.Column("description", sa.String(length=255)),
        sa.Column("suffix", sa.String(length=255)),
        sa.Column("json_schema", sa.JSON, nullable=False),
    )


def _table_target(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "Target",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String, nullable=False),
        sa.Column("short_name", sa.String, nullable=False),
        sa.Column("molecular_weight", sa.Float, nullable=True),
        sa.Column("uniprot_id", sa.String(length=64), nullable=True),
    )


def _table_sample(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "Sample",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("target_id", sa.Integer, sa.ForeignKey("Target.id"), nullable=False),
        sa.Column("average_crystal_size", sa.Float, nullable=True),
        sa.Column("crystal_shape", sa.JSON, nullable=True),
        sa.Column("created", sa.DateTime, nullable=False, server_default=func.now()),
        sa.Column("incubation_time", sa.DateTime, nullable=True),
        sa.Column("crystallization_temperature", sa.Float, nullable=True),
        sa.Column("crystal_buffer", sa.String(length=255), nullable=True),
        sa.Column("shaking_time_seconds", sa.Integer, nullable=True),
        sa.Column("shaking_strength", sa.Float, nullable=True),
        sa.Column("comment", sa.String(length=255), nullable=False, default=""),
    )


def _table_run_comment(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "RunComment",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("run_id", sa.Integer, sa.ForeignKey("Run.id")),
        sa.Column("author", sa.String(length=255), nullable=False),
        sa.Column("comment_text", sa.String(length=255), nullable=False),
        sa.Column("created", sa.DateTime(), nullable=False),
    )


def _table_proposal(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "Proposal",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("metadata", sa.JSON, nullable=True),
    )


def _table_run(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "Run",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("modified", sa.DateTime, nullable=False),
        sa.Column(
            "proposal_id",
            sa.Integer,
            sa.ForeignKey("Proposal.id"),
            nullable=False,
        ),
        sa.Column("sample_id", sa.Integer, nullable=True),
        sa.Column("karabo", sa.BLOB, nullable=True),
        sa.Column("custom", sa.JSON, nullable=False),
    )


class DBTables:
    def __init__(
        self,
        sample: sa.Table,
        proposal: sa.Table,
        run: sa.Table,
        run_comment: sa.Table,
        custom_run_property: sa.Table,
        target: sa.Table,
    ) -> None:
        self.sample = sample
        self.proposal = proposal
        self.run = run
        self.run_comment = run_comment
        self.target = target
        self.custom_run_property = custom_run_property
        self.property_comments = RunProperty("comments")
        self.property_karabo = RunProperty(self.run.c.karabo.name)
        self.property_custom = RunProperty(self.run.c.custom.name)
        self.property_run_id = RunProperty(self.run.c.id.name)
        self.property_modified = RunProperty(self.run.c.modified.name)
        self.property_sample = RunProperty(self.run.c.sample_id.name)
        self.property_proposal_id = RunProperty(self.run.c.proposal_id.name)
        self.property_types: Dict[RunProperty, RichPropertyType] = {
            self.property_run_id: PropertyInt(),
            self.property_proposal_id: PropertyInt(),
            self.property_sample: PropertySample(),
        }
        self.property_descriptions: Dict[RunProperty, str] = {
            self.property_run_id: "Run",
            self.property_proposal_id: "Proposal",
            self.property_sample: "Sample",
        }


def create_tables(context: DBContext) -> DBTables:
    return DBTables(
        sample=_table_sample(context.metadata),
        proposal=_table_proposal(context.metadata),
        run=_table_run(context.metadata),
        run_comment=_table_run_comment(context.metadata),
        custom_run_property=_table_custom_run_property(context.metadata),
        target=_table_target(context.metadata),
    )


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

        # Create samples
        first_sample_result = conn.execute(
            tables.sample.insert().values(
                target_id=first_target_id,
                average_crystal_size=1.0,
                crystal_shape=[3.0, 4.0, 5.0],
            )
        )
        conn.execute(
            tables.sample.insert().values(
                target_id=first_target_id, average_crystal_size=2.0
            )
        )
        first_sample_id = first_sample_result.inserted_primary_key[0]

        # Create run properties
        conn.execute(
            tables.custom_run_property.insert().values(
                [
                    {
                        "name": "repetition_rate",
                        "description": "Repetition Rate",
                        "suffix": "MHz",
                        "json_schema": {"type": "number"},
                    },
                    {
                        "name": "status",
                        "description": "Status",
                        "suffix": None,
                        "json_schema": {
                            "type": "string",
                            "enum": ["running", "finished"],
                        },
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
                    },
                    {
                        "name": "trains",
                        "description": "Train count",
                        "suffix": None,
                        "json_schema": {"type": "integer"},
                    },
                    {
                        "name": "injector_position_z",
                        "description": "Injector Position Z",
                        "suffix": "mm",
                        "json_schema": {"type": "number"},
                    },
                    {
                        "name": "tags",
                        "description": "Tags",
                        "suffix": None,
                        "json_schema": {
                            "type": "array",
                            "items": {"type": "string", "minLength": 1},
                        },
                    },
                    {
                        "name": "started",
                        "description": "Started",
                        "suffix": None,
                        "json_schema": {"type": "string", "format": "date-time"},
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
                    custom={
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
