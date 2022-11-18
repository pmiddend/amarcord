"""merge input parameters

Revision ID: 302344474c27
Revises: 9c1911d46539
Create Date: 2022-11-24 10:44:22.899657

"""
from enum import Enum

import sqlalchemy as sa
from alembic import op
from sqlalchemy import Column

# revision identifiers, used by Alembic.
revision = "302344474c27"
down_revision = "9c1911d46539"
branch_labels = None
depends_on = None


class MergeModel(Enum):
    UNITY = "unity"
    XSPHERE = "xsphere"
    OFFSET = "offset"
    GGPM = "ggpm"


class ScaleIntensities(Enum):
    OFF = "off"
    NORMAL = "normal"
    DEBYE_WALLER = "debyewaller"


def upgrade() -> None:
    nullable_columns = op.get_bind().engine.name != "sqlite"
    with op.batch_alter_table("MergeResult") as batch_op:  # type: ignore
        connection = op.get_bind()
        mr = sa.Table(
            "MergeResult",
            sa.MetaData(),
        )
        connection.execute(sa.delete(mr))

        batch_op.drop_column("partialator_additional")
        batch_op.add_column(
            Column("input_merge_model", sa.Enum(MergeModel), nullable=nullable_columns)
        )
        batch_op.add_column(
            Column(
                "input_scale_intensities",
                sa.Enum(ScaleIntensities),
                nullable=nullable_columns,
            )
        )
        batch_op.add_column(
            Column("input_post_refinement", sa.Boolean, nullable=nullable_columns)
        )
        batch_op.add_column(
            Column("input_iterations", sa.Integer, nullable=nullable_columns)
        )
        batch_op.add_column(
            Column("input_polarisation_angle", sa.Integer, nullable=True)
        )
        batch_op.add_column(
            Column("input_polarisation_percent", sa.Integer, nullable=True)
        )
        batch_op.add_column(Column("input_start_after", sa.Integer, nullable=True))
        batch_op.add_column(Column("input_stop_after", sa.Integer, nullable=True))
        batch_op.add_column(Column("input_rel_b", sa.Float, nullable=nullable_columns))
        batch_op.add_column(
            Column("input_no_pr", sa.Boolean, nullable=nullable_columns)
        )
        batch_op.add_column(Column("input_force_bandwidth", sa.Float, nullable=True))
        batch_op.add_column(Column("input_force_radius", sa.Float, nullable=True))
        batch_op.add_column(Column("input_force_lambda", sa.Float, nullable=True))
        batch_op.add_column(
            Column("input_no_delta_cc_half", sa.Boolean, nullable=nullable_columns)
        )
        batch_op.add_column(Column("input_max_adu", sa.Float, nullable=True))
        batch_op.add_column(
            Column("input_min_measurements", sa.Integer, nullable=nullable_columns)
        )
        batch_op.add_column(Column("input_logs", sa.Boolean, nullable=nullable_columns))
        batch_op.add_column(Column("input_min_res", sa.Float, nullable=True))
        batch_op.add_column(Column("input_push_res", sa.Float, nullable=True))
        batch_op.add_column(Column("input_w", sa.String(length=255), nullable=True))


def downgrade() -> None:
    pass
