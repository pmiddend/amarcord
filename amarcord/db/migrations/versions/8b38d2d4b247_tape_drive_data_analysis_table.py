"""tape drive data analysis table

Revision ID: 8b38d2d4b247
Revises: cc54b2d4ceae
Create Date: 2021-12-07 11:04:30.763733

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "8b38d2d4b247"
down_revision = "cc54b2d4ceae"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "AnalysisResults",
        sa.Column("directory_name", sa.String(length=255), nullable=False),
        sa.Column("run_from", sa.Integer, nullable=False),
        sa.Column("run_to", sa.Integer, nullable=False),
        sa.Column("resolution", sa.String(length=255), nullable=False),
        sa.Column("rsplit", sa.Float, nullable=False),
        sa.Column("cchalf", sa.Float, nullable=False),
        sa.Column("ccstar", sa.Float, nullable=False),
        sa.Column("snr", sa.Float, nullable=False),
        sa.Column("completeness", sa.Float, nullable=False),
        sa.Column("multiplicity", sa.Float, nullable=False),
        sa.Column("total_measurements", sa.Integer, nullable=False),
        sa.Column("unique_reflections", sa.Integer, nullable=False),
        sa.Column("wilson_b", sa.Float, nullable=False),
        sa.Column("outer_shell", sa.String(length=255), nullable=False),
        sa.Column("num_patterns", sa.Integer, nullable=False),
        sa.Column("num_hits", sa.Integer, nullable=False),
        sa.Column("indexed_patterns", sa.Integer, nullable=False),
        sa.Column("indexed_crystals", sa.Integer, nullable=False),
        sa.Column("comment", sa.String(length=255), nullable=False),
    )


def downgrade():
    op.drop_table("AnalysisResults")
