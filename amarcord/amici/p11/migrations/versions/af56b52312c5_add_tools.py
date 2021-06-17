"""Add tools

Revision ID: af56b52312c5
Revises: 21956889bfbe
Create Date: 2021-06-29 12:15:01.806847

"""
from alembic import op

# revision identifiers, used by Alembic.
from sqlalchemy import Column
from sqlalchemy import Enum

from amarcord.amici.p11.db import ReductionMethod

revision = "af56b52312c5"
down_revision = "21956889bfbe"
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column(
        "Data_Reduction",
        Column(
            "method",
            Enum(ReductionMethod, values_callable=lambda x: [e.value for e in x]),
        ),
    )


def downgrade():
    pass
