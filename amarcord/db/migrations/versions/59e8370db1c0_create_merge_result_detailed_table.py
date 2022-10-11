"""create merge result detailed table

Revision ID: 59e8370db1c0
Revises: 8107247f67a4
Create Date: 2022-10-21 14:25:48.125524

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy import ForeignKey

# revision identifiers, used by Alembic.
revision = "59e8370db1c0"
down_revision = "8107247f67a4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "MergeResultShellFom",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "merge_result_id",
            sa.Integer,
            ForeignKey("MergeResult.id", ondelete="cascade"),
            nullable=False,
        ),
        sa.Column("one_over_d_centre", sa.Float, nullable=False),
        sa.Column("nref", sa.Integer, nullable=False),
        sa.Column("d_over_a", sa.Float, nullable=False),
        sa.Column("min_res", sa.Float, nullable=False),
        sa.Column("max_res", sa.Float, nullable=False),
        sa.Column("cc", sa.Float, nullable=False),
        sa.Column("ccstar", sa.Float, nullable=False),
        sa.Column("r_split", sa.Float, nullable=False),
        sa.Column("reflections_possible", sa.Integer, nullable=False),
        sa.Column("completeness", sa.Float, nullable=False),
        sa.Column("measurements", sa.Integer, nullable=False),
        sa.Column("redundancy", sa.Float, nullable=False),
        sa.Column("snr", sa.Float, nullable=False),
        sa.Column("mean_i", sa.Float, nullable=False),
    )


def downgrade() -> None:
    pass
