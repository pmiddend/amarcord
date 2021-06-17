"""create tools

Revision ID: 21956889bfbe
Revises:
Create Date: 2021-06-28 12:08:10.149964

"""
from alembic import op

# revision identifiers, used by Alembic.
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import JSON
from sqlalchemy import String
from sqlalchemy import Text
from sqlalchemy import func

revision = "21956889bfbe"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "Tools",
        Column("id", Integer(), primary_key=True, autoincrement=True),
        Column("created", DateTime, server_default=func.now()),
        Column("name", String(length=255), nullable=False),
        Column("executable_path", Text(), nullable=False),
        Column("extra_files", JSON(), nullable=False),
        Column("command_line", Text(), nullable=False),
        Column("description", Text(), nullable=False),
    )
    op.add_column(
        "Data_Reduction",
        Column(
            "tool_id",
            Integer(),
            ForeignKey("Tools.id"),
            nullable=True,
        ),
    )
    op.add_column("Data_Reduction", Column("tool_inputs", JSON(), nullable=True))


def downgrade():
    op.drop_column("Data_Reduction", "tool_id")
    op.drop_column("Data_Reduction", "tool_inputs")
    op.drop_table("Tools")
