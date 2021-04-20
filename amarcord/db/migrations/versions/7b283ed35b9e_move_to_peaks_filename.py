"""Move to peaks filename

Revision ID: 7b283ed35b9e
Revises: 55ee43aae117
Create Date: 2021-04-20 15:36:12.432636

"""
from pathlib import Path

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "7b283ed35b9e"
down_revision = "55ee43aae117"
branch_labels = None
depends_on = None


def upgrade():
    HitFindingResult = sa.sql.table(
        "HitFindingResult",
        sa.sql.column("id", sa.Integer),
        sa.sql.column("peaks_filename", sa.Text),
        sa.sql.column("result_filename", sa.Text),
    )

    connection = op.get_bind()
    connection.execute(
        HitFindingResult.update().values(
            peaks_filename=HitFindingResult.c.result_filename
        )
    )

    for result in connection.execute(
        sa.select([HitFindingResult.c.id, HitFindingResult.c.result_filename])
    ).fetchall():
        if not result["result_filename"]:
            continue

        p = Path(result["result_filename"])

        cxis = ",".join(str(s) for s in p.parent.glob("*.cxi"))

        if not cxis:
            continue

        connection.execute(
            HitFindingResult.update()
            .values(result_filename=cxis)
            .where(HitFindingResult.c.id == result["id"])
        )


def downgrade():
    pass
