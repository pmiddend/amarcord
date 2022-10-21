"""refinement_table

Revision ID: 6d6d8c40a86d
Revises: 59e8370db1c0
Create Date: 2022-11-02 15:23:30.463217

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "6d6d8c40a86d"
down_revision = "59e8370db1c0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "RefinementResult",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "merge_result_id",
            sa.Integer,
            sa.ForeignKey("MergeResult.id", ondelete="cascade"),
        ),
        sa.Column(
            "pdb_file_id", sa.Integer, sa.ForeignKey("File.id", ondelete="cascade")
        ),
        sa.Column(
            "mtz_file_id", sa.Integer, sa.ForeignKey("File.id", ondelete="cascade")
        ),
        sa.Column("r_free", sa.Float, nullable=False),
        sa.Column("r_work", sa.Float, nullable=False),
        sa.Column("rms_bond_angle", sa.Float, nullable=False),
        sa.Column("rms_bond_length", sa.Float, nullable=False),
    )


def downgrade() -> None:
    pass
