"""add experiment type to user configuration

Revision ID: 943586e83940
Revises: ed784c5f0aaa
Create Date: 2022-10-11 12:26:56.767485

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "943586e83940"
down_revision = "ed784c5f0aaa"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("UserConfiguration") as batch_op:  # type: ignore
        batch_op.add_column(
            sa.Column(
                "current_experiment_type_id",
                sa.Integer,
                sa.ForeignKey(
                    "ExperimentType.id",
                    name="user_configuration_current_experiment_type_id",
                    ondelete="set null",
                ),
                nullable=True,
            )
        )


def downgrade() -> None:
    pass
