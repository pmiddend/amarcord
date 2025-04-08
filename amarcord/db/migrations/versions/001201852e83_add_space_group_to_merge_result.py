"""add space group to merge result

Revision ID: 001201852e83
Revises: e0b2e28ea2db
Create Date: 2025-01-30 14:46:17.535661

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "001201852e83"
down_revision = "6cd377286953"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("MergeResult") as batch_op:  # type: ignore
        batch_op.add_column(
            sa.Column(
                "space_group",
                sa.String(length=32),
                nullable=True,
            ),
        )


def downgrade() -> None:
    pass
