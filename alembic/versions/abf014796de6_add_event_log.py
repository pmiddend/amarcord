"""Add event log

Revision ID: abf014796de6
Revises: cc54b2d4ceae
Create Date: 2021-04-06 11:58:22.536169

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "abf014796de6"
down_revision = "cc54b2d4ceae"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "EventLog",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column(
            "created",
            sa.DateTime(),
            server_default=sa.func.current_timestamp(),
            nullable=False,
        ),
        sa.Column(
            "level",
            sa.Enum("INFO", "WARNING", "ERROR", name="eventloglevel"),
            nullable=False,
        ),
        sa.Column("source", sa.String(length=255), nullable=False),
        sa.Column("text", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade():
    op.drop_table("EventLog")
