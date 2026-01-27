"""dataset splitting

Revision ID: e5642a0f15c4
Revises: 340cda933bab
Create Date: 2026-01-13 12:53:09.529694

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "e5642a0f15c4"
down_revision = "340cda933bab"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("MergeResult") as batch_op:
        batch_op.add_column(
            sa.Column("custom_split", sa.Text, nullable=True),
        )
        batch_op.add_column(
            sa.Column(
                "dataset", sa.String(length=255), nullable=False, server_default=""
            ),
        )


def downgrade() -> None:
    pass
