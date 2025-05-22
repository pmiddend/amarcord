"""add geometry table

Revision ID: 340cda933bab
Revises: bb5c96f181f3
Create Date: 2025-05-07 14:52:48.472891

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "340cda933bab"
down_revision = "bb5c96f181f3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "Geometry",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "beamtime_id",
            sa.Integer,
            sa.ForeignKey(
                "Beamtime.id", ondelete="cascade", name="geometry_has_beamtime_id_fk"
            ),
            nullable=False,
        ),
        sa.Column(
            "content",
            sa.Text,
            nullable=False,
        ),
        sa.Column(
            "hash",
            sa.String(length=64),
            nullable=False,
        ),
        sa.Column(
            "name",
            sa.String(length=255),
            nullable=False,
        ),
        sa.Column(
            "created",
            sa.DateTime,
            nullable=False,
        ),
    )
    op.create_table(
        "GeometryReferencesAttributo",
        sa.Column(
            "geometry_id",
            sa.Integer,
            sa.ForeignKey("Geometry.id", ondelete="cascade"),
            nullable=False,
        ),
        sa.Column(
            "attributo_id",
            sa.Integer,
            sa.ForeignKey("Attributo.id", ondelete="cascade"),
            nullable=False,
        ),
    )
    op.create_table(
        "IndexingResultGeometryTemplateReplacement",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "indexing_result_id",
            sa.Integer,
            sa.ForeignKey("IndexingResult.id", ondelete="cascade"),
            nullable=False,
        ),
        sa.Column(
            "attributo_id",
            sa.Integer,
            sa.ForeignKey("Attributo.id", ondelete="cascade"),
            nullable=False,
        ),
        sa.Column("replacement", sa.String, nullable=False),
    )
    with op.batch_alter_table("IndexingParameters") as batch_op:  # type: ignore
        batch_op.add_column(
            sa.Column(
                "geometry_id",
                sa.Integer,
                sa.ForeignKey(
                    "Geometry.id",
                    ondelete="cascade",
                    name="indexing_parameters_geometry_fk",
                ),
                nullable=True,
            ),
        )
    with op.batch_alter_table("IndexingParameters") as batch_op:  # type: ignore
        batch_op.drop_column("geometry_file")
    with op.batch_alter_table("IndexingResult") as batch_op:  # type: ignore
        batch_op.add_column(
            sa.Column(
                "generated_geometry_id",
                sa.Integer,
                sa.ForeignKey(
                    "Geometry.id",
                    ondelete="cascade",
                    name="indexing_result_generated_geometry_fk",
                ),
                nullable=True,
            ),
        )
        # Remove geometry ID column
        batch_op.drop_column("geometry_file")
        batch_op.drop_column("geometry_hash")
        batch_op.drop_column("generated_geometry_file")


def downgrade() -> None:
    pass
