"""Create peaks filename

Revision ID: 55ee43aae117
Revises: cc54b2d4ceae
Create Date: 2021-04-20 15:15:37.461758

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "55ee43aae117"
down_revision = "cc54b2d4ceae"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "HitFindingResult", sa.Column("peaks_filename", sa.Text, nullable=True)
    )


def downgrade():
    op.drop_column("HitFindingResult", "peaks_filename")
