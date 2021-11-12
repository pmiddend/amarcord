import pytest

from amarcord.db.db import DB
from amarcord.db.tables import create_tables
from amarcord.modules.dbcontext import CreationMode
from amarcord.modules.dbcontext import DBContext


@pytest.fixture
def db() -> DB:
    db_context = DBContext("sqlite://", echo=False)
    tables = create_tables(db_context)
    db_context.create_all(creation_mode=CreationMode.DONT_CHECK)
    return DB(db_context, tables)
