"""add beamtime analysis output path

Revision ID: e0b2e28ea2db
Revises: 517e71fa7a19
Create Date: 2025-01-15 07:50:26.038746

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "e0b2e28ea2db"
down_revision = "517e71fa7a19"
branch_labels = None
depends_on = None

BEAMTIME_TABLE_NEW = sa.sql.table(
    "Beamtime",
    sa.column("analysis_output_path", sa.Text),
)


def upgrade() -> None:
    with op.batch_alter_table("Beamtime") as batch_op:  # type: ignore
        batch_op.add_column(
            sa.Column(
                "analysis_output_path",
                sa.Text,
                nullable=True,
            ),
        )

    connection = op.get_bind()

    connection.execute(
        BEAMTIME_TABLE_NEW.update().values(
            {
                "analysis_output_path": "/asap3/petra3/gpfs/{beamtime.beamline_lowercase}/{beamtime.year}/data/{beamtime.external_id}/processed",
            },
        ),
    )

    with op.batch_alter_table("Beamtime") as batch_op:  # type: ignore
        batch_op.alter_column(
            "analysis_output_path",
            existing_type=sa.Text,
            nullable=False,
        )


def downgrade() -> None:
    pass
