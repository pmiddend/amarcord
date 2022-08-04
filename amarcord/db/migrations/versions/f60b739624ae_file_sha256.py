"""file_sha256

Revision ID: f60b739624ae
Revises: 341b2d1ddb48
Create Date: 2022-03-31 14:45:46.939423

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f60b739624ae"
down_revision = "341b2d1ddb48"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("File") as batch_op:  # type: ignore
        batch_op.alter_column(
            "sha256", existing_type=sa.String(length=40), type_=sa.String(length=64)
        )


def downgrade() -> None:
    with op.batch_alter_table("File") as batch_op:  # type: ignore
        batch_op.alter_column(
            "sha256", existing_type=sa.String(length=64), type_=sa.String(length=40)
        )
