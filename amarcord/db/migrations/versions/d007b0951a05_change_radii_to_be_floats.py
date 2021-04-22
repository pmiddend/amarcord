"""Change radii to be floats

Revision ID: d007b0951a05
Revises: 7b283ed35b9e
Create Date: 2021-04-22 10:26:18.597300

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "d007b0951a05"
down_revision = "7b283ed35b9e"
branch_labels = None
depends_on = None


def upgrade():
    # Reason for this: SQLite doesn't support alter table:
    # https://alembic.sqlalchemy.org/en/latest/batch.html
    with op.batch_alter_table("IntegrationParameters") as batch_op:
        batch_op.alter_column("radius_inner", type_=sa.Float)
        batch_op.alter_column("radius_middle", type_=sa.Float)
        batch_op.alter_column("radius_outer", type_=sa.Float)


def downgrade():
    with op.batch_alter_table("IntegrationParameters") as batch_op:
        batch_op.alter_column("radius_inner", type_=sa.Integer)
        batch_op.alter_column("radius_middle", type_=sa.Integer)
        batch_op.alter_column("radius_outer", type_=sa.Integer)
