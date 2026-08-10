"""export jobs

Revision ID: 83534ff1e2b9
Revises: e5642a0f15c4
Create Date: 2026-07-16 14:01:53.356604

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "83534ff1e2b9"
down_revision = "e5642a0f15c4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ExportJob",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "beamtime_id",
            sa.Integer,
            sa.ForeignKey(
                "Beamtime.id", ondelete="cascade", name="export_job_has_beamtime_id_fk"
            ),
            nullable=False,
        ),
        sa.Column(
            "created",
            sa.DateTime,
            nullable=False,
        ),
        sa.Column(
            "started",
            sa.DateTime,
            nullable=True,
        ),
        sa.Column(
            "stopped",
            sa.DateTime,
            nullable=True,
        ),
        sa.Column("contains_stream_files", sa.Boolean, nullable=False),
        sa.Column(
            "size_in_mebibytes",
            sa.Integer,
            nullable=True,
        ),
        sa.Column(
            "output_path",
            sa.Text,
            nullable=False,
        ),
        sa.Column(
            "status_message",
            sa.Text,
            nullable=False,
        ),
    )


def downgrade() -> None:
    pass
