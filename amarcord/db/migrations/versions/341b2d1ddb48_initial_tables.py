# pylint: disable=trailing-whitespace
"""initial tables

Revision ID: 341b2d1ddb48
Revises: 
Create Date: 2022-03-23 16:33:43.172942

"""
from enum import Enum

import sqlalchemy as sa
from alembic import op
from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy.sql import column
from sqlalchemy.sql import table

# revision identifiers, used by Alembic.
revision = "341b2d1ddb48"
down_revision = None
branch_labels = None
depends_on = None


class EventLogLevel(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class AssociatedTable(Enum):
    RUN = "run"
    SAMPLE = "sample"


def upgrade() -> None:
    op.create_table(
        "Run",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("modified", sa.DateTime, nullable=False),
        sa.Column("attributi", sa.JSON, nullable=False),
    )

    op.create_table(
        "Attributo",
        sa.Column("name", sa.String(length=255), primary_key=True),
        sa.Column("description", sa.String(length=255), nullable=False),
        sa.Column("group", sa.String(length=255), nullable=False),
        sa.Column(
            "associated_table",
            sa.Enum(AssociatedTable),
            nullable=False,
        ),
        sa.Column("json_schema", sa.JSON, nullable=False),
    )

    op.create_table(
        "File",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("type", sa.String(length=255), nullable=False),
        sa.Column("file_name", sa.String(length=255), nullable=False),
        sa.Column("size_in_bytes", sa.Integer(), nullable=False),
        sa.Column("original_path", sa.Text(), nullable=True),
        sa.Column("sha256", sa.String(length=40), nullable=False),
        sa.Column("modified", sa.DateTime(), nullable=False),
        sa.Column("contents", sa.LargeBinary(), nullable=False),
        sa.Column("description", sa.String(length=255)),
    )

    op.create_table(
        "DataSet",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("experiment_type", sa.String(length=255), nullable=False),
        sa.Column("attributi", sa.JSON, nullable=False),
    )

    op.create_table(
        "ExperimentHasAttributo",
        sa.Column("experiment_type", sa.String(length=255), nullable=False),
        sa.Column(
            "attributo_name",
            sa.String(length=255),
            # If the attributo vanishes, delete the experiment type with it
            ForeignKey("Attributo.name", ondelete="cascade", onupdate="cascade"),
            nullable=False,
        ),
    )

    op.create_table(
        "Sample",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("modified", sa.DateTime, nullable=False),
        sa.Column("attributi", sa.JSON, nullable=False),
    )

    op.create_table(
        "CFELAnalysisResults",
        sa.Column("directory_name", sa.String(length=255), nullable=False),
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "data_set_id",
            sa.Integer,
            # If the data set vanishes, delete the corresponding analysis result as well
            ForeignKey("DataSet.id", ondelete="cascade"),
            nullable=False,
        ),
        sa.Column("resolution", sa.String(length=255), nullable=False),
        sa.Column("rsplit", sa.Float, nullable=False),
        sa.Column("cchalf", sa.Float, nullable=False),
        sa.Column("ccstar", sa.Float, nullable=False),
        sa.Column("snr", sa.Float, nullable=False),
        sa.Column("completeness", sa.Float, nullable=False),
        sa.Column("multiplicity", sa.Float, nullable=False),
        sa.Column("total_measurements", sa.Integer, nullable=False),
        sa.Column("unique_reflections", sa.Integer, nullable=False),
        sa.Column("num_patterns", sa.Integer, nullable=False),
        sa.Column("num_hits", sa.Integer, nullable=False),
        sa.Column("indexed_patterns", sa.Integer, nullable=False),
        sa.Column("indexed_crystals", sa.Integer, nullable=False),
        sa.Column("crystfel_version", sa.String(length=64), nullable=False),
        sa.Column("ccstar_rsplit", sa.Float, nullable=True),
        sa.Column("created", sa.DateTime(), nullable=False),
    )

    op.create_table(
        "CFELAnalysisResultHasFile",
        sa.Column(
            "analysis_result_id",
            sa.Integer(),
            # If the analysis result vanishes, delete this row here
            ForeignKey("CFELAnalysisResults.id", ondelete="cascade"),
        ),
        sa.Column(
            "file_id",
            sa.Integer(),
            # If the file vanishes (why would it?), delete this entry
            ForeignKey("File.id", ondelete="cascade"),
        ),
    )

    op.create_table(
        "SampleHasFile",
        sa.Column(
            "sample_id",
            sa.Integer(),
            # If the sample vanishes, delete this entry as well
            ForeignKey("Sample.id", ondelete="cascade"),
        ),
        sa.Column(
            "file_id",
            sa.Integer(),
            # If the file vanishes, delete this entry as well
            ForeignKey("File.id", ondelete="cascade"),
        ),
    )

    op.create_table(
        "RunHasFile",
        sa.Column(
            "run_id",
            sa.Integer(),
            # If the run vanishes, delete this entry as well
            ForeignKey("Run.id", ondelete="cascade"),
        ),
        sa.Column(
            "file_id",
            sa.Integer(),
            # If the file vanishes, delete this entry as well
            ForeignKey("File.id", ondelete="cascade"),
        ),
    )

    op.create_table(
        "EventLog",
        sa.Column(
            "id",
            sa.Integer,
            primary_key=True,
        ),
        sa.Column("created", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.Column("level", sa.Enum(EventLogLevel), nullable=False),
        sa.Column("source", sa.String(length=255), nullable=False),
        sa.Column("text", sa.Text, nullable=False),
    )

    attributo = table(
        "Attributo",
        column("name", String),
        column("description", String),
        column("group", String),
        column("associated_table", sa.Enum(AssociatedTable)),
        column("json_schema", sa.JSON),
    )

    for column_name in ("started", "stopped"):
        # noinspection PyTypeChecker
        op.execute(
            attributo.insert().values(  # type: ignore
                name=column_name,
                description="",
                group="internal",
                associated_table=AssociatedTable.RUN,
                json_schema={"type": "integer", "format": "date-time"},
            )
        )


def downgrade() -> None:
    pass
