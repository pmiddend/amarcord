"""Add beamtime schedule table

Revision ID: ae30cc10b443
Revises: 5ce49cef4117
Create Date: 2022-09-13 14:00:11.349280

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy import ForeignKey

# revision identifiers, used by Alembic.
revision = "ae30cc10b443"
down_revision = "5ce49cef4117"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "BeamtimeSchedule",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "sample_id",
            sa.Integer(),
            # If the sample vanishes, delete this entry as well
            ForeignKey("Sample.id", ondelete="cascade"),
        ),
        sa.Column("users", sa.String(length=255), nullable=False),
        sa.Column("td_support", sa.String(length=255), nullable=False),
        sa.Column("comment", sa.Text(), nullable=False),
        sa.Column("shift", sa.String(length=255), nullable=False),
        sa.Column("date", sa.String(length=10), nullable=False),
    )


def downgrade() -> None:
    pass
