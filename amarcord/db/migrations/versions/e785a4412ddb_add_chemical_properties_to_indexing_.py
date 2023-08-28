"""add chemical properties to indexing results

Revision ID: e785a4412ddb
Revises: 943586e83940
Create Date: 2022-10-13 10:55:16.823600

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy import ForeignKey
from sqlalchemy.sql import column
from sqlalchemy.sql import select
from sqlalchemy.sql import table

# revision identifiers, used by Alembic.
revision = "e785a4412ddb"
down_revision = "f2720d6108a5"
branch_labels = None
depends_on = None

_indexing_result = table(
    "IndexingResult",
    column("id", sa.Integer),
    column("run_id", sa.Integer),
    column("cell_description", sa.String(length=255)),
    column("point_group", sa.String(length=32)),
    column("chemical_id", sa.Integer),
)

_run = table(
    "Run",
    column("id", sa.Integer),
    column("attributi", sa.JSON),
)

_chemical = table(
    "Chemical",
    column("id", sa.Integer),
    column("attributi", sa.JSON),
)


def upgrade() -> None:
    with op.batch_alter_table("IndexingResult") as batch_op:  # type: ignore
        batch_op.add_column(
            sa.Column("cell_description", sa.String(length=255), nullable=True)
        )
        batch_op.add_column(
            sa.Column("point_group", sa.String(length=32), nullable=True)
        )
        batch_op.add_column(
            sa.Column(
                "chemical_id",
                sa.Integer,
                ForeignKey(
                    "Chemical.id",
                    ondelete="cascade",
                    name="indexing_result_has_chemical_fk",
                ),
                nullable=True,
            )
        )

    conn = op.get_bind()
    for row in conn.execute(
        select(_indexing_result.c.id, _run.c.attributi).join(
            _run, _run.c.id == _indexing_result.c.run_id
        )
    ).fetchall():
        ir_id = row[0]
        run_attributi = row[1]

        protein_id = run_attributi.get("protein")
        if protein_id is None:
            raise Exception(
                f"indexing result {ir_id} has run without protein set, cannot migrate"
            )
        assert isinstance(
            protein_id, int
        ), f"protein ID is not an integer but {protein_id}"

        protein = conn.execute(
            select(_chemical.c.attributi).where(_chemical.c.id == protein_id)
        ).one_or_none()

        if protein is None:
            raise Exception(
                f"indexing result {ir_id} has protein {protein_id} which we cannot find; cannot migrate"
            )

        protein_attributi = protein[0]

        cell_description = protein_attributi.get("cell description")
        point_group = protein_attributi.get("point group")

        op.execute(
            _indexing_result.update()
            .where(_indexing_result.c.id == ir_id)
            .values(
                {
                    "cell_description": cell_description,
                    "point_group": point_group,
                    "chemical_id": protein_id,
                }
            )
        )

    with op.batch_alter_table("IndexingResult") as batch_op:  # type: ignore
        batch_op.alter_column("chemical_id", existing_type=sa.Integer, nullable=False)


def downgrade() -> None:
    pass
