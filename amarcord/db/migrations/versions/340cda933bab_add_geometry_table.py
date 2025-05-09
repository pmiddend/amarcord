"""add geometry table

Revision ID: 340cda933bab
Revises: bb5c96f181f3
Create Date: 2025-05-07 14:52:48.472891

"""

from alembic import op
import sqlalchemy as sa


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
        # sa.Column(
        #     "parent_geometry",
        #     sa.Integer,
        #     sa.ForeignKey(
        #             "Geometry.id",
        #             ondelete="cascade",
        #             name="geometry_parent_fk",
        #     ),
        #     nullable=True,
        # ),
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
    with op.batch_alter_table("IndexingResult") as batch_op:  # type: ignore
        batch_op.add_column(
            sa.Column(
                "geometry_id",
                sa.Integer,
                sa.ForeignKey(
                    "Geometry.id",
                    ondelete="cascade",
                    name="indexing_result_geometry_fk",
                ),
                nullable=True,
            ),
        )
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


def downgrade() -> None:
    pass
