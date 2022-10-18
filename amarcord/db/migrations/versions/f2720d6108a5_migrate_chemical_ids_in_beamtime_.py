"""Migrate chemical ids in beamtime schedule

Revision ID: f2720d6108a5
Revises: f0282df086bf
Create Date: 2022-10-13 18:49:38.965160

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy import ForeignKey

# revision identifiers, used by Alembic.
revision = "f2720d6108a5"
down_revision = "943586e83940"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bsc = op.create_table(
        "BeamtimeScheduleHasChemical",
        sa.Column(
            "beamtime_schedule_id",
            sa.Integer(),
            # If the beamtime shift vanishes, delete this entry as well
            ForeignKey("BeamtimeSchedule.id", ondelete="cascade"),
        ),
        sa.Column(
            "chemical_id",
            sa.Integer(),
            # If the chemical vanishes, delete this entry as well
            ForeignKey("Chemical.id", ondelete="cascade"),
        ),
        sa.PrimaryKeyConstraint(
            "beamtime_schedule_id", "chemical_id", name="BeamtimeScheduleHasChemical_pk"
        ),
    )

    assert bsc is not None

    bs = sa.Table(
        "BeamtimeSchedule",
        sa.MetaData(),
        sa.Column("id", sa.Integer),
        sa.Column("sample_id", sa.Integer),
    )

    connection = op.get_bind()
    results = connection.execute(
        sa.select(
            [
                bs.c.id,
                bs.c.sample_id,
            ]
        ).filter(bs.c.sample_id.isnot(None))
    ).fetchall()
    for schedule_id, chemical_id in results:
        connection.execute(
            bsc.insert().values(
                {"beamtime_schedule_id": schedule_id, "chemical_id": chemical_id}
            )
        )

    naming_convention = {
        "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    }
    with op.batch_alter_table(  # type: ignore
        "BeamtimeSchedule", naming_convention=naming_convention
    ) as batch_op:
        if op.get_bind().engine.name == "mysql":
            batch_op.drop_constraint("BeamtimeSchedule_ibfk_1", type_="foreignkey")
        batch_op.drop_column("sample_id")


def downgrade() -> None:
    pass
