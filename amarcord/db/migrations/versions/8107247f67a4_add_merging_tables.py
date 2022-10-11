"""add merging tables

Revision ID: 8107247f67a4
Revises: e785a4412ddb
Create Date: 2022-10-17 13:07:58.137577

"""
from enum import Enum

import sqlalchemy as sa
from alembic import op
from sqlalchemy import ForeignKey

# revision identifiers, used by Alembic.
revision = "8107247f67a4"
down_revision = "e785a4412ddb"
branch_labels = None
depends_on = None


class MergeNegativeHandling(Enum):
    IGNORE = "ignore"
    ZERO = "zero"


class DBJobStatus(Enum):
    QUEUED = "queued"
    RUNNING = "running"
    DONE = "done"


def upgrade() -> None:
    op.create_table(
        "MergeResult",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("created", sa.DateTime, nullable=False),
        sa.Column("partialator_additional", sa.Text, nullable=False),
        sa.Column("recent_log", sa.Text, nullable=False),
        sa.Column("negative_handling", sa.Enum(MergeNegativeHandling), nullable=True),
        sa.Column("job_status", sa.Enum(DBJobStatus), nullable=False),
        sa.Column("started", sa.DateTime, nullable=True),
        sa.Column("stopped", sa.DateTime, nullable=True),
        sa.Column("point_group", sa.String(length=32), nullable=True),
        sa.Column("cell_description", sa.String(length=255), nullable=True),
        sa.Column("job_id", sa.Integer, nullable=True),
        sa.Column("job_error", sa.Text, nullable=True),
        sa.Column(
            "mtz_file_id",
            sa.Integer,
            ForeignKey("File.id", ondelete="cascade"),
            nullable=True,
        ),
        sa.Column("fom_snr", sa.Float, nullable=True),
        sa.Column("fom_wilson", sa.Float, nullable=True),
        sa.Column("fom_ln_k", sa.Float, nullable=True),
        sa.Column("fom_discarded_reflections", sa.Integer, nullable=True),
        sa.Column("fom_one_over_d_from", sa.Float, nullable=True),
        sa.Column("fom_one_over_d_to", sa.Float, nullable=True),
        sa.Column("fom_redundancy", sa.Float, nullable=True),
        sa.Column("fom_completeness", sa.Float, nullable=True),
        sa.Column("fom_measurements_total", sa.Integer, nullable=True),
        sa.Column("fom_reflections_total", sa.Integer, nullable=True),
        sa.Column("fom_reflections_possible", sa.Integer, nullable=True),
        sa.Column("fom_r_split", sa.Float, nullable=True),
        sa.Column("fom_r1i", sa.Float, nullable=True),
        sa.Column("fom_2", sa.Float, nullable=True),
        sa.Column("fom_cc", sa.Float, nullable=True),
        sa.Column("fom_ccstar", sa.Float, nullable=True),
        sa.Column("fom_ccano", sa.Float, nullable=True),
        sa.Column("fom_crdano", sa.Float, nullable=True),
        sa.Column("fom_rano", sa.Float, nullable=True),
        sa.Column("fom_rano_over_r_split", sa.Float, nullable=True),
        sa.Column("fom_d1sig", sa.Float, nullable=True),
        sa.Column("fom_d2sig", sa.Float, nullable=True),
        sa.Column("fom_outer_resolution", sa.Float, nullable=True),
        sa.Column("fom_outer_ccstar", sa.Float, nullable=True),
        sa.Column("fom_outer_r_split", sa.Float, nullable=True),
        sa.Column("fom_outer_cc", sa.Float, nullable=True),
        sa.Column("fom_outer_unique_reflections", sa.Integer, nullable=True),
        sa.Column("fom_outer_completeness", sa.Float, nullable=True),
        sa.Column("fom_outer_redundancy", sa.Float, nullable=True),
        sa.Column("fom_outer_snr", sa.Float, nullable=True),
        sa.Column("fom_outer_min_res", sa.Float, nullable=True),
        sa.Column("fom_outer_max_res", sa.Float, nullable=True),
    )

    op.create_table(
        "MergeResultHasIndexingResult",
        sa.Column(
            "merge_result_id",
            sa.Integer,
            ForeignKey("MergeResult.id", ondelete="cascade"),
        ),
        sa.Column(
            "indexing_result_id",
            sa.Integer,
            ForeignKey("IndexingResult.id", ondelete="cascade"),
        ),
    )


def downgrade() -> None:
    pass
