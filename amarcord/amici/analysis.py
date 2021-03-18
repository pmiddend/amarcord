import os
from pathlib import Path

from amarcord.db.db import DB
from amarcord.db.tables import create_tables
from amarcord.modules.dbcontext import CreationMode, DBContext


def ingest_cheetah() -> None:
    dbcontext = DBContext(
        "sqlite:////" + str(Path(os.environ["HOME"]) / "amarcord-test.sqlite")
    )
    tables = create_tables(dbcontext)
    DB(dbcontext, tables)

    dbcontext.create_all(creation_mode=CreationMode.CHECK_FIRST)

    print("ingesting")
