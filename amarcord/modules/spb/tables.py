import datetime
from dataclasses import dataclass
from typing import Dict, Optional

import sqlalchemy as sa

from amarcord.modules.dbcontext import DBContext
from amarcord.modules.spb.column import RunProperty


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
        sa.Column("modified", sa.DateTime, nullable=False),
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
    )


@dataclass(frozen=True)
class Tables:
    sample: sa.Table
    proposal: sa.Table
    run_tag: sa.Table
    run: sa.Table
    run_comment: sa.Table


def create_tables(context: DBContext) -> Tables:
    return Tables(
        sample=_table_sample(context.metadata),
        proposal=_table_proposal(context.metadata),
        run_tag=_table_run_tag(context.metadata),
        run=_table_run(context.metadata),
        run_comment=_table_run_comment(context.metadata),
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
        conn.execute(
            tables.proposal.insert().values(
                id=2, metadata={"data": {}, "title": "shit proposal"}
            )
        )
        first_sample_result = conn.execute(
            tables.sample.insert().values(sample_name="first sample")
        )
        conn.execute(tables.sample.insert().values(sample_name="second sample"))
        first_sample_id = first_sample_result.inserted_primary_key[0]

        run_id = conn.execute(
            tables.run.insert().values(
                proposal_id=proposal_id,
                started=datetime.datetime.now(),
                modified=datetime.datetime.now(),
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
                modified=datetime.datetime.now(),
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


def run_property_atomic_db_columns(tables: Tables) -> Dict[RunProperty, sa.Column]:
    return {
        RunProperty.RUN_ID: tables.run.c.id,
        RunProperty.STATUS: tables.run.c.status,
        RunProperty.SAMPLE: tables.run.c.sample_id,
        RunProperty.STARTED: tables.run.c.started,
        RunProperty.REPETITION_RATE: tables.run.c.repetition_rate_mhz,
        RunProperty.PULSE_ENERGY: tables.run.c.pulse_energy_mj,
        RunProperty.HIT_RATE: tables.run.c.hit_rate,
        RunProperty.INDEXING_RATE: tables.run.c.indexing_rate,
        RunProperty.X_RAY_ENERGY: tables.run.c.xray_energy_kev,
        RunProperty.INJECTOR_POSITION_Z_MM: tables.run.c.injector_position_z_mm,
        RunProperty.DETECTOR_DISTANCE_MM: tables.run.c.detector_distance_mm,
        RunProperty.INJECTOR_FLOW_RATE: tables.run.c.injector_flow_rate,
        RunProperty.TRAINS: tables.run.c.trains,
        RunProperty.SAMPLE_DELIVERY_RATE: tables.run.c.sample_delivery_rate,
    }
