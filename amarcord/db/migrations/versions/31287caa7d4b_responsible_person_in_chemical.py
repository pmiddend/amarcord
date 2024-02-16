"""responsible person in chemical

Revision ID: 31287caa7d4b
Revises: 9c1911d46539
Create Date: 2023-01-27 09:24:13.240287

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy import Column

# revision identifiers, used by Alembic.
revision = "31287caa7d4b"
down_revision = "302344474c27"
branch_labels = None
depends_on = None


_ATTRIBUTO_RESPONSIBLE_PERSON = "responsible person"
_CHEMICAL_COLUMN_RESPONSIBLE_PERSON = "responsible_person"
_chemical = sa.sql.table(
    "Chemical",
    sa.column("id", sa.Integer),
    sa.column("name", sa.String(length=255)),
    sa.column("attributi", sa.JSON),
    sa.column("modified", sa.DateTime),
    sa.column("type", sa.String),
    sa.column(_CHEMICAL_COLUMN_RESPONSIBLE_PERSON, sa.String(length=255)),
)

_attributo = sa.sql.table(
    "Attributo",
    sa.column("name", sa.String(length=255)),
)


def upgrade() -> None:
    with op.batch_alter_table("Chemical") as batch_op:  # type: ignore
        batch_op.add_column(
            Column(
                _CHEMICAL_COLUMN_RESPONSIBLE_PERSON,
                sa.String(length=255),
                nullable=True,
            )
        )

    conn = op.get_bind()
    if (
        conn.execute(
            sa.select(_attributo.c.name).where(
                _attributo.c.name == _ATTRIBUTO_RESPONSIBLE_PERSON
            )
        ).fetchone()
        is not None
    ):
        for row in conn.execute(
            sa.select(_chemical.c.id, _chemical.c.name, _chemical.c.attributi)
        ).fetchall():
            responsible_person_attributo = row[2].get(_ATTRIBUTO_RESPONSIBLE_PERSON)
            if responsible_person_attributo is not None:
                row[2].pop(_ATTRIBUTO_RESPONSIBLE_PERSON)
                op.execute(
                    _chemical.update()
                    .where(_chemical.c.id == row[0])
                    .values(
                        responsible_person=responsible_person_attributo,
                        attributi=row[2],
                    )
                )
            else:
                op.execute(
                    _chemical.update()
                    .where(_chemical.c.id == row[0])
                    .values(responsible_person="No responsible person")
                )
        with op.batch_alter_table("Attributo") as batch_op:  # type: ignore
            batch_op.execute(
                _attributo.delete().where(
                    _attributo.c.name == _ATTRIBUTO_RESPONSIBLE_PERSON
                )
            )
    else:
        # If we don't have an attributo for responsible person, simply set it to 'none' for all chemcials.
        op.execute(
            _chemical.update()
            .values(responsible_person="No responsible person")
        )


    with op.batch_alter_table("Chemical") as batch_op:  # type: ignore
        batch_op.alter_column(
            _CHEMICAL_COLUMN_RESPONSIBLE_PERSON,
            existing_type=sa.String(length=255),
            nullable=False,
        )


def downgrade() -> None:
    pass
