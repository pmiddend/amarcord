"""add online crystfel

Revision ID: 79fe95fa663e
Revises: ae30cc10b443
Create Date: 2022-09-13 15:06:49.400912

"""
from enum import Enum

import sqlalchemy as sa
from alembic import op
from sqlalchemy import Column
from sqlalchemy import ForeignKey

# revision identifiers, used by Alembic.
revision = "79fe95fa663e"
down_revision = "ae30cc10b443"
branch_labels = None
depends_on = None


class DBJobStatus(Enum):
    QUEUED = "queued"
    RUNNING = "running"
    DONE = "done"


def upgrade() -> None:
    with op.batch_alter_table("UserConfiguration") as batch_op:  # type: ignore
        batch_op.add_column(Column("use_online_crystfel", sa.Boolean, nullable=False))

    op.drop_table("CFELAnalysisResultHasFile")
    op.drop_table("CFELAnalysisResults")
    op.create_table(
        "IndexingResult",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("created", sa.DateTime, nullable=False),
        sa.Column(
            "run_id",
            sa.Integer,
            ForeignKey("Run.id", ondelete="cascade"),
        ),
        sa.Column("stream_file", sa.Text(), nullable=True),
        sa.Column("frames", sa.Integer(), nullable=True),
        sa.Column("hits", sa.Integer(), nullable=True),
        sa.Column("not_indexed_frames", sa.Integer(), nullable=True),
        sa.Column("hit_rate", sa.Float(), nullable=False),
        sa.Column("indexing_rate", sa.Float(), nullable=False),
        sa.Column("indexed_frames", sa.Integer(), nullable=False),
        sa.Column("job_id", sa.Integer, nullable=True),
        sa.Column("job_status", sa.Enum(DBJobStatus), nullable=False),
        sa.Column("job_error", sa.Text, nullable=True),
    )


def downgrade() -> None:
    pass
