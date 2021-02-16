from dataclasses import dataclass
import datetime
import sqlalchemy as sa
from amarcord.modules.dbcontext import DBContext


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
        sa.Column("run_id", sa.Integer, sa.ForeignKey("Run.id")),
        sa.Column("author", sa.String(length=255), nullable=False),
        sa.Column("text", sa.String(length=255), nullable=False),
        sa.Column("created", sa.DateTime(), nullable=False),
    )


def _table_run(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "Run",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("modified", sa.DateTime, nullable=False),
        sa.Column("status", sa.String(length=255), nullable=False),
        sa.Column("sample_id", sa.Integer, nullable=True),
        sa.Column("repetition_rate_mhz", sa.Float, nullable=False),
        sa.Column("pulse_energy_mj", sa.Float, nullable=False),
        # sa.Column("pulses_per_train", sa.Integer, nullable=False),
        # sa.Column("xray_energy_kev", sa.Float, nullable=False),
        # sa.Column("injector_position_z", sa.Float, nullable=False),
    )


@dataclass(frozen=True)
class Tables:
    sample: sa.Table
    run_tag: sa.Table
    run: sa.Table
    run_comment: sa.Table


def create_tables(context: DBContext) -> Tables:
    return Tables(
        _table_sample(context.metadata),
        _table_run_tag(context.metadata),
        _table_run(context.metadata),
        _table_run_comment(context.metadata),
    )


def create_sample_data(context: DBContext, tables: Tables) -> None:
    with context.connect() as conn:
        first_sample_result = conn.execute(
            tables.sample.insert().values(sample_name="first sample")
        )
        conn.execute(tables.sample.insert().values(sample_name="second sample"))
        first_sample_id = first_sample_result.inserted_primary_key[0]

        run_id = conn.execute(
            tables.run.insert().values(
                modified=datetime.datetime.now(),
                status="finished",
                sample_id=first_sample_id,
                repetition_rate_mhz=3.5,
                pulse_energy_mj=1,
            )
        ).inserted_primary_key[0]

        conn.execute(tables.run_tag.insert().values(run_id=run_id, tag_text="t1"))
        conn.execute(tables.run_tag.insert().values(run_id=run_id, tag_text="t2"))

        conn.execute(
            tables.run.insert().values(
                modified=datetime.datetime.now(),
                status="running",
                sample_id=first_sample_id,
                repetition_rate_mhz=4.3,
                pulse_energy_mj=2,
            )
        )
