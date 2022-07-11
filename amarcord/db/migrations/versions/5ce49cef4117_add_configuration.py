"""add configuration

Revision ID: 5ce49cef4117
Revises: e57d716c41a1
Create Date: 2022-04-28 16:19:11.472700

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "5ce49cef4117"
down_revision = "e57d716c41a1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "UserConfiguration",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("created", sa.DateTime, nullable=False),
        sa.Column("auto_pilot", sa.Boolean, nullable=False),
    )


def downgrade() -> None:
    op.drop_table("UserConfiguration")
