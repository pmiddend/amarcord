"""add compression to files

Revision ID: 7cfe057d4cfd
Revises: 75d6fd044afa
Create Date: 2025-04-28 09:21:19.381372

"""

import zlib

import sqlalchemy as sa
from alembic import op
from sqlalchemy.sql import select

# revision identifiers, used by Alembic.
revision = "7cfe057d4cfd"
down_revision = "75d6fd044afa"
branch_labels = None
depends_on = None

NEW_FILE_TABLE = sa.sql.table(
    "File",
    sa.column("id", sa.Integer),
    sa.column("contents", sa.LargeBinary),
    sa.column("size_in_bytes", sa.Integer),
    sa.column("size_in_bytes_compressed", sa.Integer),
)


def upgrade() -> None:
    with op.batch_alter_table("File") as batch_op:  # type: ignore
        batch_op.add_column(
            sa.Column(
                "size_in_bytes_compressed",
                sa.Integer,
                nullable=True,
            ),
        )

    connection = op.get_bind()
    for file_id, size_in_bytes, file_contents in connection.execute(
        select(
            NEW_FILE_TABLE.c.id,
            NEW_FILE_TABLE.c.size_in_bytes,
            NEW_FILE_TABLE.c.contents,
        ),
    ).fetchall():  # pragma: no cover
        #  Heuristically determined value for the minimum sensible
        # file size where compression (with gz) makes sense
        if size_in_bytes < 1000:
            continue

        new_contents = zlib.compress(file_contents)
        new_size = len(new_contents)

        connection.execute(
            NEW_FILE_TABLE.update()
            .where(NEW_FILE_TABLE.c.id == file_id)
            .values({"contents": new_contents, "size_in_bytes_compressed": new_size}),
        )


def downgrade() -> None:
    pass
