"""add experiment type to run

Revision ID: 9ccfa582f374
Revises: 31287caa7d4b
Create Date: 2023-08-28 10:01:24.416308

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "9ccfa582f374"
down_revision = "31287caa7d4b"
branch_labels = None
depends_on = None

run_to_experiment_type_id: dict[str, list[tuple[tuple[int, int], int]]] = {
    # We have to add stuff here before we migrate old databases
    # "tapedrive_2023_04": [((19, 52), 4), ((53, 59), 8), ((60, 136), 4)]
}

_run = sa.sql.table(
    "Run",
    sa.column("id", sa.Integer),
    sa.column("experiment_type_id", sa.Integer),
)


def upgrade() -> None:
    conn = op.get_bind()
    engine_url = conn.engine.url

    remaining_run_ids: set[int] = set(
        row[0] for row in conn.execute(sa.select(_run.c.id)).fetchall()
    )

    run_assignments: None | list[tuple[tuple[int, int], int]] = next(
        iter(
            items
            for db_name, items in run_to_experiment_type_id.items()
            if db_name in engine_url
        ),
        None,
    )

    if remaining_run_ids and run_assignments is None:
        raise Exception(
            'cannot add "experiment_type_id" column to Run table: we have runs in the DB, but no assignments from Run to experiment type'
        )

    if run_assignments is not None:
        for (run_range_begin, run_range_end), _ in run_assignments:
            remaining_run_ids -= set(range(run_range_begin, run_range_end + 1))

        if remaining_run_ids:
            raise Exception(
                'cannot add "experiment_type_id" column to Run table: the following runs have no experiment type assignment: '
                + ", ".join(str(i) for i in remaining_run_ids)
            )

    with op.batch_alter_table("Run") as batch_op:  # type: ignore
        batch_op.add_column(
            sa.Column(
                "experiment_type_id",
                sa.Integer,
                sa.ForeignKey(
                    "ExperimentType.id",
                    name="run_experiment_type_id",
                ),
                nullable=True,
            )
        )

    if run_assignments is not None:
        for run_range, experiment_type_id in run_assignments:
            for run_id in range(run_range[0], run_range[1] + 1):
                op.execute(
                    _run.update()
                    .where(_run.c.id == run_id)
                    .values(experiment_type_id=experiment_type_id)
                )

    with op.batch_alter_table("Run") as batch_op:  # type: ignore
        batch_op.alter_column(
            "experiment_type_id",
            existing_type=sa.Integer,
            nullable=False,
        )


def downgrade() -> None:
    pass
