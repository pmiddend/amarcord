"""add align detector table

Revision ID: bb5c96f181f3
Revises: ef7e87df02b8
Create Date: 2025-04-29 11:07:43.565907

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.sql import select

# revision identifiers, used by Alembic.
revision = "bb5c96f181f3"
down_revision = "ef7e87df02b8"
branch_labels = None
depends_on = None

_ALIGN_DETECTOR_GROUP_TABLE = sa.sql.table(
    "AlignDetectorGroup",
    sa.column("group", sa.String),
    sa.Column(
        "indexing_result_id",
        sa.Integer,
    ),
    sa.Column(
        "x_translation_mm",
        sa.Float,
    ),
    sa.Column(
        "y_translation_mm",
        sa.Float,
    ),
    sa.Column(
        "z_translation_mm",
        sa.Float,
    ),
    sa.Column(
        "x_rotation_deg",
        sa.Float,
    ),
    sa.Column(
        "y_rotation_deg",
        sa.Float,
    ),
)

_INDEXING_RESULT_TABLE = sa.sql.table(
    "IndexingResult",
    sa.column("id", sa.Integer()),
    sa.Column(
        "detector_shift_x_mm",
        sa.Float,
    ),
    sa.Column(
        "detector_shift_y_mm",
        sa.Float,
    ),
)


def upgrade() -> None:
    op.create_table(
        "AlignDetectorGroup",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "indexing_result_id",
            sa.Integer,
            sa.ForeignKey(
                "IndexingResult.id",
                ondelete="cascade",
                name="idnexing_result_has_align_detector_group_fk",
            ),
            nullable=False,
        ),
        sa.Column(
            "group",
            sa.String(length=255),
            nullable=False,
        ),
        sa.Column(
            "x_translation_mm",
            sa.Float(),
            nullable=False,
        ),
        sa.Column(
            "y_translation_mm",
            sa.Float(),
            nullable=False,
        ),
        sa.Column(
            "z_translation_mm",
            sa.Float(),
            nullable=True,
        ),
        sa.Column(
            "x_rotation_deg",
            sa.Float(),
            nullable=True,
        ),
        sa.Column(
            "y_rotation_deg",
            sa.Float(),
            nullable=True,
        ),
    )

    connection = op.get_bind()

    for (
        indexing_result_id,
        detector_shift_x_mm,
        detector_shift_y_mm,
    ) in connection.execute(
        select(
            _INDEXING_RESULT_TABLE.c.id,
            _INDEXING_RESULT_TABLE.c.detector_shift_x_mm,
            _INDEXING_RESULT_TABLE.c.detector_shift_y_mm,
        ),
    ).fetchall():  # pragma: no cover
        if detector_shift_x_mm is not None and detector_shift_y_mm is not None:
            connection.execute(
                _ALIGN_DETECTOR_GROUP_TABLE.insert().values(
                    {
                        "indexing_result_id": indexing_result_id,
                        "group": "all",
                        "x_translation_mm": detector_shift_x_mm,
                        "y_translation_mm": detector_shift_y_mm,
                    }
                )
            )

    with op.batch_alter_table("IndexingResult") as batch_op:  # type: ignore
        batch_op.drop_column("detector_shift_x_mm")
        batch_op.drop_column("detector_shift_y_mm")


def downgrade() -> None:
    pass
