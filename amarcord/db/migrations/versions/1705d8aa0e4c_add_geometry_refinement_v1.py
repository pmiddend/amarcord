"""add_geometry_refinement_v1

Revision ID: 1705d8aa0e4c
Revises: 9ccfa582f374
Create Date: 2023-09-08 11:31:18.333155

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "1705d8aa0e4c"
down_revision = "9ccfa582f374"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("IndexingResult") as batch_op:  # type: ignore
        batch_op.add_column(
            sa.Column(
                "detector_shift_x_mm",
                sa.Float,
                nullable=True,
            )
        )
        batch_op.add_column(
            sa.Column(
                "detector_shift_y_mm",
                sa.Float,
                nullable=True,
            )
        )


def downgrade() -> None:
    pass
