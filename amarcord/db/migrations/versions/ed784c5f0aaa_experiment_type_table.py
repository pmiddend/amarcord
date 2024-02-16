"""experiment type in user config

Revision ID: ed784c5f0aaa
Revises: e5fd0907cc9b
Create Date: 2022-10-11 09:38:47.171341

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.sql import column
from sqlalchemy.sql import select
from sqlalchemy.sql import table

# revision identifiers, used by Alembic.
revision = "ed784c5f0aaa"
down_revision = "e5fd0907cc9b"
branch_labels = None
depends_on = None

_experiment_has_attributo_old = table(
    "ExperimentHasAttributo",
    column("experiment_type", sa.String(length=255)),
)

_experiment_has_attributo_big = table(
    "ExperimentHasAttributo",
    column("experiment_type", sa.String(length=255)),
    column("experiment_type_id", sa.Integer),
)

_data_set_big = table(
    "DataSet",
    column("experiment_type", sa.String(length=255)),
    column("experiment_type_id", sa.Integer),
)


def upgrade() -> None:
    # Create the new table. Previously we didn't have it, we only had ExperimentHasAttributo with an experiment type
    # string column.
    experiment_type = op.create_table(
        "ExperimentType",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String(length=255), nullable=False, unique=True),
    )

    assert experiment_type is not None

    # Get all current experiment types and insert them into the table, get the ID
    conn = op.get_bind()
    name_to_id: dict[str, int] = {}
    # Set, because we might have duplicates (the table is "experiment type has attributo", not
    # "experiment types" after all). However, we want a definite order for the IDs of the experiment types, so we sort
    existing_experiment_type_names: list[str] = list(set(
        row[0]
        for row in conn.execute(
            select(_experiment_has_attributo_old.c.experiment_type)
        ).fetchall()
    ))
    existing_experiment_type_names.sort(key=str.casefold)
    for experiment_type_name in existing_experiment_type_names:
        name_to_id[experiment_type_name] = conn.execute(
            experiment_type.insert().values({"name": experiment_type_name})
        ).inserted_primary_key[0]

    # Table ExperimentHasAttributo
    # Add the experiment_type_id column as nullable
    with op.batch_alter_table("ExperimentHasAttributo") as batch_op:  # type: ignore
        batch_op.add_column(
            sa.Column(
                "experiment_type_id",
                sa.Integer,
                sa.ForeignKey(
                    "ExperimentType.id",
                    name="experiment_has_attributo_et_id",
                    ondelete="cascade",
                ),
                nullable=True,
            )
        )

    # Update all entries with its ID
    for et_name, et_id in name_to_id.items():
        conn.execute(
            _experiment_has_attributo_big.update()
            .where(_experiment_has_attributo_big.c.experiment_type == et_name)
            .values({"experiment_type_id": et_id})
        )

    with op.batch_alter_table("ExperimentHasAttributo") as batch_op:  # type: ignore
        # Remove the old experiment_type string column
        batch_op.drop_column("experiment_type")

    # Table DataSet
    # Add the experiment_type_id column as nullable
    with op.batch_alter_table("DataSet") as batch_op:  # type: ignore
        batch_op.add_column(
            sa.Column(
                "experiment_type_id",
                sa.Integer,
                sa.ForeignKey(
                    "ExperimentType.id",
                    name="data_set_et_id",
                    ondelete="cascade",
                ),
                nullable=True,
            )
        )

    # Update all entries with its ID
    for et_name, et_id in name_to_id.items():
        conn.execute(
            _data_set_big.update()
            .where(_data_set_big.c.experiment_type == et_name)
            .values({"experiment_type_id": et_id})
        )

    with op.batch_alter_table("DataSet") as batch_op:  # type: ignore
        # Remove the old experiment_type string column
        batch_op.drop_column("experiment_type")

    # Set the experiment_type_id column as non-nullable
    with op.batch_alter_table("ExperimentHasAttributo") as batch_op:  # type: ignore
        batch_op.alter_column(
            "experiment_type_id", existing_type=sa.Integer, nullable=False
        )

    # Set the experiment_type_id column as non-nullable
    with op.batch_alter_table("DataSet") as batch_op:  # type: ignore
        batch_op.alter_column(
            "experiment_type_id", existing_type=sa.Integer, nullable=False
        )


def downgrade() -> None:
    pass
