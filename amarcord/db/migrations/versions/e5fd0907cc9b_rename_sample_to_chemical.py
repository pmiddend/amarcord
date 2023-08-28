"""rename sample to chemical

Revision ID: e5fd0907cc9b
Revises: 79fe95fa663e
Create Date: 2022-10-10 13:38:48.162644

"""
from enum import Enum

import sqlalchemy as sa
from alembic import op
from sqlalchemy.sql import column
from sqlalchemy.sql import select
from sqlalchemy.sql import table

# revision identifiers, used by Alembic.
revision = "e5fd0907cc9b"
down_revision = "79fe95fa663e"
branch_labels = None
depends_on = None


class AssociatedTableOld(Enum):
    RUN = "run"
    SAMPLE = "sample"


class AssociatedTableBig(Enum):
    RUN = "run"
    SAMPLE = "sample"
    CHEMICAL = "chemical"


class AssociatedTableNew(Enum):
    RUN = "run"
    CHEMICAL = "chemical"


_attributo = table(
    "Attributo",
    column("name", sa.String(length=255)),
    column("description", sa.String(length=255)),
    column("group", sa.String(length=255)),
    column(
        "associated_table",
        sa.Enum(AssociatedTableBig),
    ),
    column("json_schema", sa.JSON),
)


def upgrade() -> None:
    op.rename_table("Sample", "Chemical")
    op.rename_table("SampleHasFile", "ChemicalHasFile")

    with op.batch_alter_table("ChemicalHasFile") as batch_op:  # type: ignore
        batch_op.alter_column(
            column_name="sample_id",
            new_column_name="chemical_id",
            existing_type=sa.Integer,
        )

    # In MySQL, we cannot directly change an enum's value (as far as we know), so we
    # 1. add "chemical" as a possible enum value
    # 2. modify all rows from "sample" to "chemical"
    # 3. remove "sample" as a possible value
    with op.batch_alter_table("Attributo") as batch_op:  # type: ignore
        batch_op.alter_column(
            "associated_table",
            existing_type=sa.Enum(AssociatedTableOld),
            type_=sa.Enum(AssociatedTableBig),
        )
    op.execute(
        _attributo.update()
        .where(_attributo.c.associated_table == AssociatedTableBig.SAMPLE)
        .values({"associated_table": AssociatedTableBig.CHEMICAL})
    )
    with op.batch_alter_table("Attributo") as batch_op:  # type: ignore
        batch_op.alter_column(
            "associated_table",
            existing_type=sa.Enum(AssociatedTableBig),
            type_=sa.Enum(AssociatedTableNew),
        )

    # In the attributo table, the json_schema might contain something like this:
    #
    # { "type": "integer", "format": "sample-id" }
    #
    # This, we need to change to "chemical-id" as well.
    conn = op.get_bind()
    for row in conn.execute(
        select(_attributo.c.name, _attributo.c.json_schema)
    ).fetchall():
        json_schema = row[1]
        if json_schema["type"] == "integer":
            json_format = json_schema.get("format")
            if json_format is not None and json_format == "sample-id":
                json_schema["format"] = "chemical-id"
                op.execute(
                    _attributo.update()
                    .where(_attributo.c.name == row[0])
                    .values({"json_schema": json_schema})
                )


def downgrade() -> None:
    pass
