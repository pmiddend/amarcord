import datetime
from enum import Enum, auto
from typing import Any, Dict, List, Optional

import sqlalchemy as sa

from amarcord.modules.dbcontext import DBContext
from amarcord.modules.spb.run_property import RunProperty
from amarcord.qt.properties import (
    PropertyChoice,
    PropertyDateTime,
    PropertyDouble,
    PropertyInt,
    PropertySample,
    PropertyTags,
    RichPropertyType,
)


class CustomRunPropertyType(Enum):
    DOUBLE = auto()
    STRING = auto()


def _table_custom_run_property(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "CustomRunProperty",
        metadata,
        sa.Column("name", sa.String(length=255), primary_key=True),
        sa.Column("prop_type", sa.Enum(CustomRunPropertyType), nullable=False),
    )


def _table_sample(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "Sample",
        metadata,
        sa.Column("sample_id", sa.Integer, primary_key=True),
        sa.Column("sample_name", sa.String(length=255)),
    )


def _table_run_tag(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "RunTag",
        metadata,
        sa.Column("run_id", sa.Integer, sa.ForeignKey("Run.id")),
        sa.Column("tag_text", sa.String(length=255), nullable=False),
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
        sa.Column(
            "proposal_id",
            sa.Integer,
            sa.ForeignKey("Proposal.id"),
            nullable=False,
        ),
        sa.Column("started", sa.DateTime, nullable=False),
        sa.Column("status", sa.String(length=255), nullable=False),
        sa.Column("sample_id", sa.Integer, nullable=True),
        sa.Column("repetition_rate_mhz", sa.Float, nullable=True),
        sa.Column("pulse_energy_mj", sa.Float, nullable=True),
        sa.Column("hit_rate", sa.Float, nullable=True),
        sa.Column("indexing_rate", sa.Float, nullable=True),
        sa.Column("karabo", sa.BLOB, nullable=True),
        sa.Column("xray_energy_kev", sa.Float, nullable=True),
        sa.Column("injector_position_z_mm", sa.Float, nullable=True),
        sa.Column("trains", sa.Integer, nullable=True),
        sa.Column("sample_delivery_rate", sa.Float, nullable=True),
        sa.Column("detector_distance_mm", sa.Float, nullable=True),
        sa.Column("injector_flow_rate", sa.Float, nullable=True),
        sa.Column("custom", sa.JSON, nullable=True),
    )


class Tables:
    def __init__(
        self,
        sample: sa.Table,
        proposal: sa.Table,
        run_tag: sa.Table,
        run: sa.Table,
        run_comment: sa.Table,
        custom_run_property: sa.Table,
    ) -> None:
        self.sample = sample
        self.proposal = proposal
        self.run_tag = run_tag
        self.run = run
        self.run_comment = run_comment
        self.custom_run_property = custom_run_property
        self.property_tags = RunProperty("tags")
        self.property_comments = RunProperty("comments")
        self.property_karabo = RunProperty(self.run.c.karabo.name)
        self.property_custom = RunProperty(self.run.c.custom.name)
        self.property_started = RunProperty(self.run.c.started.name)
        self.property_status = RunProperty(self.run.c.status.name)
        self.property_run_id = RunProperty(self.run.c.id.name)
        self.property_sample = RunProperty(self.run.c.sample_id.name)
        self.property_started = RunProperty(self.run.c.started.name)
        self.property_proposal_id = RunProperty(self.run.c.proposal_id.name)
        self.manual_properties = {self.property_tags, self.property_sample}
        self.property_types: Dict[RunProperty, RichPropertyType] = {
            self.property_run_id: PropertyInt(),
            self.property_proposal_id: PropertyInt(),
            self.property_tags: PropertyTags(),
            self.property_status: PropertyChoice(
                values=[("finished", "finished"), ("running", "running")]
            ),
            self.property_sample: PropertySample(),
            RunProperty(self.run.c.repetition_rate_mhz.name): PropertyDouble(),
            RunProperty(self.run.c.detector_distance_mm.name): PropertyDouble(
                suffix="mm"
            ),
            RunProperty(self.run.c.hit_rate.name): PropertyDouble(range=(0.0, 100.0)),
            RunProperty(self.run.c.indexing_rate.name): PropertyDouble(
                range=(0.0, 100.0)
            ),
            RunProperty(self.run.c.injector_flow_rate.name): PropertyDouble(
                nonNegative=True, suffix="μL/min"
            ),
            RunProperty(self.run.c.pulse_energy_mj.name): PropertyDouble(
                nonNegative=True, suffix="mJ"
            ),
            RunProperty(self.run.c.injector_position_z_mm.name): PropertyDouble(
                suffix="mm"
            ),
            RunProperty(self.run.c.sample_delivery_rate.name): PropertyDouble(
                nonNegative=True, suffix="uL/min"
            ),
            self.property_started: PropertyDateTime(),
            RunProperty(self.run.c.trains.name): PropertyInt(nonNegative=True),
            RunProperty(self.run.c.xray_energy_kev.name): PropertyDouble(
                nonNegative=True, suffix="keV"
            ),
        }

    def run_property_to_string(self, r: RunProperty, v: Any) -> str:
        if v is None:
            return "None"
        run_prop_type = self.property_types.get(r, None)
        if isinstance(run_prop_type, PropertyTags):
            assert isinstance(v, list)
            return ", ".join(v)
        if isinstance(v, datetime.datetime):
            return v.strftime("%Y-%m-%d %H:%M:%S")
        if not isinstance(v, (int, float, str, bool)):
            raise Exception(f"run property {r} has invalid type {type(v)}")
        result = str(v)
        suffix = getattr(run_prop_type, "suffix", None)
        if suffix is not None:
            if not isinstance(suffix, str):
                raise Exception(f"got a suffix of type {(type(suffix))}")
            result += f" {suffix}"
        return result


def create_tables(context: DBContext) -> Tables:
    return Tables(
        sample=_table_sample(context.metadata),
        proposal=_table_proposal(context.metadata),
        run_tag=_table_run_tag(context.metadata),
        run=_table_run(context.metadata),
        run_comment=_table_run_comment(context.metadata),
        custom_run_property=_table_custom_run_property(context.metadata),
    )


def create_sample_data(context: DBContext, tables: Tables) -> None:
    karabo_data: Optional[bytes]
    try:
        with open("data/pickled_karabo", "rb") as f:
            karabo_data = f.read()
    except:
        karabo_data = None

    with context.connect() as conn:
        proposal_id = 1
        conn.execute(
            tables.proposal.insert().values(
                id=proposal_id, metadata={"data": {}, "title": "test proposal"}
            )
        )
        # conn.execute(
        #     tables.proposal.insert().values(
        #         id=2, metadata={"data": {}, "title": "shit proposal"}
        #     )
        # )
        first_sample_result = conn.execute(
            tables.sample.insert().values(sample_name="first sample")
        )
        conn.execute(tables.sample.insert().values(sample_name="second sample"))
        first_sample_id = first_sample_result.inserted_primary_key[0]

        run_id = conn.execute(
            tables.run.insert().values(
                proposal_id=proposal_id,
                started=datetime.datetime.now(),
                status="finished",
                sample_id=first_sample_id,
                repetition_rate_mhz=3.5,
                pulse_energy_mj=1,
                hit_rate=0.5,
                indexing_rate=0.8,
                xray_energy_kev=1.2,
                injector_position_z_mm=10,
                injector_flow_rate=5.0,
                trains=1000,
                sample_delivery_rate=102,
                karabo=karabo_data,
                detector_distance_mm=10.0,
            )
        ).inserted_primary_key[0]

        conn.execute(tables.run_tag.insert().values(run_id=run_id, tag_text="t1"))
        conn.execute(tables.run_tag.insert().values(run_id=run_id, tag_text="t2"))

        conn.execute(
            tables.run.insert().values(
                proposal_id=proposal_id,
                started=datetime.datetime.now() + datetime.timedelta(seconds=1),
                status="running",
                sample_id=first_sample_id,
                repetition_rate_mhz=4.3,
                pulse_energy_mj=2,
                hit_rate=0.9,
                indexing_rate=0.2,
                xray_energy_kev=3.2,
                injector_position_z_mm=12,
                injector_flow_rate=6.0,
                trains=1001,
                sample_delivery_rate=103,
                karabo=karabo_data,
                detector_distance_mm=10.0,
            )
        )


def run_property_db_columns(tables: Tables) -> List[sa.Column]:
    return list(tables.run.c.values())


def run_property_atomic_db_columns(tables: Tables) -> List[sa.Column]:
    return [
        tables.run.c.id,
        tables.run.c.status,
        tables.run.c.sample_id,
        tables.run.c.started,
        tables.run.c.repetition_rate_mhz,
        tables.run.c.pulse_energy_mj,
        tables.run.c.hit_rate,
        tables.run.c.indexing_rate,
        tables.run.c.xray_energy_kev,
        tables.run.c.injector_position_z_mm,
        tables.run.c.detector_distance_mm,
        tables.run.c.injector_flow_rate,
        tables.run.c.trains,
        tables.run.c.sample_delivery_rate,
    ]
