"""change merge result log to long text

Revision ID: 6cd377286953
Revises: 001201852e83
Create Date: 2025-01-30 14:47:37.424237

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.mysql import LONGTEXT

# revision identifiers, used by Alembic.
revision = "6cd377286953"
down_revision = "e0b2e28ea2db"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("MergeResult") as batch_op:  # type: ignore
        batch_op.alter_column(
            "recent_log",
            existing_type=sa.Text,
            existing_nullable=True,
            existing_server_default=None,
            type_=sa.Text().with_variant(LONGTEXT, "mysql"),
        )


def downgrade() -> None:
    pass
