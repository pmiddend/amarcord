from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import DateTime


def run_table(metadata: MetaData) -> Table:
    return Table(
        "Run",
        metadata,
        Column("run_id", Integer, primary_key=True),
        Column("started", DateTime, nullable=False),
    )
