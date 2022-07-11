"""file longblob

Revision ID: 5b8dc84f3333
Revises: b0c34b3508af
Create Date: 2022-04-04 15:37:40.966678

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
from sqlalchemy.dialects.mysql import LONGBLOB

revision = "5b8dc84f3333"
down_revision = "b0c34b3508af"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("File") as batch_op:  # type: ignore
        # See https://stackoverflow.com/questions/43791725/sqlalchemy-how-to-make-a-longblob-column-in-mysql
        batch_op.alter_column(
            "contents",
            existing_type=sa.LargeBinary(),
            type_=sa.LargeBinary().with_variant(LONGBLOB, "mysql"),
        )


def downgrade() -> None:
    pass
