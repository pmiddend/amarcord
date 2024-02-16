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
    # 1 is "simple xtal", 2 is "time-resolved"
    "tapedrive_2022_04": [((35, 102), 1), ((103, 123), 2)],
    # 1 is "ligand mixing", 5 is "RadDam pro", 6 is "simple SSX"
    "tapedrive_2022_05": [
        ((23, 50), 6),
        ((51, 109), 5),
        ((110, 112), 1),
        ((113, 114), 5),
        ((115, 117), 6),
        ((118, 122), 5),
    ],
    "tapedrive_2022_07": [
        # 2 is time-resolved SSX
        ((5, 63), 2)
    ],
    "tapedrive_2022_09": [
        # 1 is sample-based, 2 is time-resolved SSX
        ((6, 46), 1),
        ((47, 50), 2),
        ((51, 56), 1),
        ((57, 71), 2),
        ((72, 75), 1),
        ((76, 84), 2),
        ((85, 89), 1),
        ((90, 101), 2),
        ((102, 126), 1),
        ((127, 130), 2),
        ((131, 134), 1),
        ((135, 142), 2),
        ((143, 143), 1),
        ((144, 144), 2),
        ((145, 145), 1),
        ((146, 146), 2),
        ((147, 173), 1),
    ],
    "tapedrive_2022_11": [
        ((1, 41), 1),
        ((42, 45), 3),
        ((46, 65), 2),
        ((66, 83), 3),
        ((84, 96), 1),
        ((97, 154), 3),
        ((155, 165), 1),
    ],
    "tapedrive_2023_04": [
        ((19, 52), 4),
        ((53, 77), 8),
        ((78, 137), 9),
    ],
    "tapedrive_2023_09": [
        ((11, 26), 1),
        ((27, 101), 6),
        ((102, 111), 1),
        ((112, 138), 3),
        ((139, 163), 1),
        ((164, 177), 3),
        ((178, 179), 1),
        ((180, 181), 3),
        ((182, 191), 6),
        ((192, 193), 7),
        ((194, 207), 6),
        ((208, 210), 7),
        ((211, 225), 6),
    ],
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
