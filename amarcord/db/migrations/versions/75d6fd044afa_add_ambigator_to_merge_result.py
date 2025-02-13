"""add ambigator to merge result

Revision ID: 75d6fd044afa
Revises: 001201852e83
Create Date: 2025-02-11 07:35:39.299335

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "75d6fd044afa"
down_revision = "001201852e83"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("MergeResult") as batch_op:  # type: ignore
        batch_op.add_column(
            sa.Column(
                "ambigator_command_line",
                sa.String(length=255),
                nullable=True,
            ),
        )
        batch_op.add_column(
            sa.Column(
                "ambigator_fg_graph_file_id",
                sa.Integer,
                nullable=True,
            ),
        )


def downgrade() -> None:
    pass
