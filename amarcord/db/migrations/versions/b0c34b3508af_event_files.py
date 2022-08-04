"""event_files

Revision ID: b0c34b3508af
Revises: f60b739624ae
Create Date: 2022-03-31 13:49:11.899040

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
from sqlalchemy import ForeignKey

revision = "b0c34b3508af"
down_revision = "f60b739624ae"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "EventHasFile",
        sa.Column(
            "event_id",
            sa.Integer(),
            # If the run vanishes, delete this entry as well
            ForeignKey("EventLog.id", ondelete="cascade"),
        ),
        sa.Column(
            "file_id",
            sa.Integer(),
            # If the file vanishes, delete this entry as well
            ForeignKey("File.id", ondelete="cascade"),
        ),
    )


def downgrade() -> None:
    op.drop_table("EventHasFile")
