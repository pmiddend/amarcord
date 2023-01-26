"""chemical types

Revision ID: 9c1911d46539
Revises: 6d6d8c40a86d
Create Date: 2023-01-17 15:20:19.461426

"""
from enum import Enum

import sqlalchemy as sa
from alembic import op
from sqlalchemy import Column
from sqlalchemy.sql import column
from sqlalchemy.sql import table

# revision identifiers, used by Alembic.
revision = "9c1911d46539"
down_revision = "6d6d8c40a86d"
branch_labels = None
depends_on = None


class ChemicalType(Enum):
    CRYSTAL = "crystal"
    SOLUTION = "solution"


_chemical = table(
    "Chemical",
    column("name", sa.String(length=255)),
    column("type", sa.Enum(ChemicalType)),
)

_experiment_has_attributo = table(
    "ExperimentHasAttributo",
    column("attributo_name", sa.String(length=255)),
    column("chemical_role", sa.Enum(ChemicalType)),
)

_HARDCODED_SOLUTION_CHEMICALS = [
    "lysozyme",
    "Chitopentaose",
    "Ethyl gallate",
    "Ixazomib",
    "Boric Acid",
    "CHC pH10",
    "CHC ph4",
    "CTO",
    "Lyso Xtal buffer",
    "Lysozyme solution",
    "Sodium Dithionite 20 mM",
    "Sodium Dithionite",
    "Sodium Dithionite 1M",
    "pH3 mix",
    "CPO",
    "NaCl 500 mM",
    "NaCl 1M",
    "JINXED crystallizing agent CTO",
    "Br-JInxed",
    "Chitotetraose",
    "CuCl2",
]


def upgrade() -> None:
    # We cannot directly set the column to non-null, because what value would we use for the chemicals?
    # So we set it to nullable, set the value, and then set it to non-null
    with op.batch_alter_table("Chemical") as batch_op:  # type: ignore
        batch_op.add_column(Column("type", sa.Enum(ChemicalType), nullable=True))

    op.execute(
        _chemical.update()
        .where(_chemical.c.name.in_(_HARDCODED_SOLUTION_CHEMICALS))
        .values({"type": ChemicalType.SOLUTION})
    )
    op.execute(
        _chemical.update()
        .where(_chemical.c.type.is_(None))
        .values({"type": ChemicalType.CRYSTAL})
    )
    with op.batch_alter_table("Chemical") as batch_op:  # type: ignore
        batch_op.alter_column(
            "type", existing_type=sa.Enum(ChemicalType), nullable=False
        )

    # Same here
    with op.batch_alter_table("ExperimentHasAttributo") as batch_op:  # type: ignore
        batch_op.add_column(
            Column("chemical_role", sa.Enum(ChemicalType), nullable=True)
        )

    op.execute(
        _experiment_has_attributo.update()
        .where(_experiment_has_attributo.c.attributo_name.like("%protein%"))
        .values({"chemical_role": ChemicalType.CRYSTAL})
    )

    op.execute(
        _experiment_has_attributo.update()
        .where(_experiment_has_attributo.c.chemical_role.is_(None))
        .values({"chemical_role": ChemicalType.SOLUTION})
    )

    with op.batch_alter_table("ExperimentHasAttributo") as batch_op:  # type: ignore
        batch_op.alter_column(
            "chemical_role", existing_type=sa.Enum(ChemicalType), nullable=False
        )


def downgrade() -> None:
    pass
