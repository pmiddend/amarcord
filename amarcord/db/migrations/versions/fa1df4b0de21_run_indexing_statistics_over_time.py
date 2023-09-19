"""run indexing statistics over time

Revision ID: fa1df4b0de21
Revises: 1705d8aa0e4c
Create Date: 2023-09-11 08:08:26.955811

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy import ForeignKey

# revision identifiers, used by Alembic.
revision = "fa1df4b0de21"
down_revision = "1705d8aa0e4c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "IndexingResultHasStatistic",
        sa.Column(
            "indexing_result_id",
            sa.Integer(),
            # If the run vanishes, delete this entry as well
            ForeignKey("IndexingResult.id", ondelete="cascade"),
        ),
        sa.Column("time", sa.DateTime(), nullable=False),
        sa.Column("frames", sa.Integer(), nullable=False),
        sa.Column("hits", sa.Integer(), nullable=False),
        sa.Column("indexed_frames", sa.Integer(), nullable=False),
        sa.Column("indexed_crystals", sa.Integer(), nullable=False),
    )


def downgrade() -> None:
    pass
