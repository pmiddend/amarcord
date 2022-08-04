"""update cascades attributi

Revision ID: e57d716c41a1
Revises: 5b8dc84f3333
Create Date: 2022-04-06 10:05:52.392190

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "e57d716c41a1"
down_revision = "5b8dc84f3333"
branch_labels = None
depends_on = None

# see https://alembic.sqlalchemy.org/en/latest/batch.html#dropping-unnamed-or-named-foreign-key-constraints
naming_convention = {
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
}


def upgrade() -> None:
    with op.batch_alter_table(  # type: ignore
        "ExperimentHasAttributo", naming_convention=naming_convention
    ) as batch_op:
        if op.get_bind().engine.name == "mysql":
            batch_op.drop_constraint(
                "ExperimentHasAttributo_ibfk_1", type_="foreignkey"
            )
            batch_op.create_foreign_key(
                constraint_name=None,
                referent_table="Attributo",
                local_cols=["attributo_name"],
                remote_cols=["name"],
                onupdate="cascade",
                ondelete="cascade",
            )


def downgrade() -> None:
    pass
