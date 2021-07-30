"""create tools

Revision ID: 21956889bfbe
Revises:
Create Date: 2021-06-28 12:08:10.149964

"""
from enum import Enum

import sqlalchemy as sa
from alembic import context
from alembic import op

# revision identifiers, used by Alembic.
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import ForeignKey
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy import Integer
from sqlalchemy import JSON
from sqlalchemy import String
from sqlalchemy import Text
from sqlalchemy import func

revision = "21956889bfbe"
down_revision = None
branch_labels = None
depends_on = None


class JobStatus(Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"


def upgrade():
    if (
        context.get_x_argument(as_dictionary=True).get("analysis-schema", None)
        is not None
    ):
        print("Not creating any tables, since we have an analysis schema")
        return
    print("Creating v1 tools tables")
    op.create_table(
        "Tools",
        Column("id", Integer(), primary_key=True, autoincrement=True),
        Column("created", DateTime, server_default=func.now()),
        Column("name", String(length=255), nullable=False),
        Column("executable_path", Text(), nullable=False),
        Column("extra_files", JSON(), nullable=False),
        Column("command_line", Text(), nullable=False),
        Column("description", Text(), nullable=False),
    )
    op.create_table(
        "Jobs",
        Column("id", Integer(), primary_key=True),
        Column("queued", DateTime, nullable=False),
        Column("started", DateTime, nullable=True),
        Column("stopped", DateTime, nullable=True),
        Column("status", sa.Enum(JobStatus), nullable=False),
        Column("failure_reason", Text(), nullable=True),
        Column("output_directory", Text(), nullable=True),
        Column(
            "tool_id",
            Integer(),
            ForeignKey("Tools.id"),
            nullable=True,
        ),
        Column("tool_inputs", JSON(), nullable=True),
        Column("metadata", JSON(), nullable=True),
    )
    op.create_table(
        "Job_To_Diffraction",
        Column("job_id", Integer(), ForeignKey("Jobs.id"), primary_key=True),
        Column(
            "crystal_id",
            String(length=255),
            ForeignKey("Crystals.crystal_id"),
            primary_key=True,
        ),
        Column(
            "run_id",
            Integer(),
            primary_key=True,
        ),
        ForeignKeyConstraint(
            ["crystal_id", "run_id"], ["Diffractions.crystal_id", "Diffractions.run_id"]
        ),
    )
    op.create_table(
        "Job_To_Data_Reduction",
        Column("job_id", Integer(), ForeignKey("Jobs.id"), primary_key=True),
        Column(
            "data_reduction_id",
            Integer(),
            ForeignKey("Data_Reduction.data_reduction_id"),
            primary_key=True,
        ),
    )


def downgrade():
    if (
        context.get_x_argument(as_dictionary=True).get("analysis-schema", None)
        is not None
    ):
        return

    op.drop_table("Tools")
    op.drop_table("Jobs")
    op.drop_table("Job_To_Diffraction")
    op.drop_table("Job_To_Data_Reduction")
