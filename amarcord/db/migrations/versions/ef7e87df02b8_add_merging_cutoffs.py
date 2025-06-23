"""add merging cutoffs

Revision ID: ef7e87df02b8
Revises: 7cfe057d4cfd
Create Date: 2025-04-28 14:08:41.116836

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "ef7e87df02b8"
down_revision = "7cfe057d4cfd"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("MergeResult") as batch_op:  # type: ignore
        batch_op.add_column(
            sa.Column(
                "cutoff_lowres",
                sa.Float,
                nullable=True,
            ),
        )
        batch_op.add_column(
            sa.Column(
                "cutoff_highres",
                sa.String(length=255),
                nullable=True,
            ),
        )


def downgrade() -> None:
    pass
