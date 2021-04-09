import logging
import os
from pathlib import Path

from amarcord.amici.analysis import AMARCORD_DB_ENV_VAR
from amarcord.amici.analysis import ingest_cheetah
from amarcord.db.db import DB
from amarcord.db.proposal_id import ProposalId
from amarcord.db.raw_attributi_map import RawAttributiMap
from amarcord.db.table_classes import DBSample
from amarcord.db.tables import create_tables
from amarcord.modules.dbcontext import CreationMode
from amarcord.modules.dbcontext import DBContext

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

test_db_filename = "/tmp/testdb.sqlite"

dbcontext = DBContext(f"sqlite:///{test_db_filename}")
tables = create_tables(dbcontext)
dbcontext.create_all(CreationMode.CHECK_FIRST)
db = DB(dbcontext, tables)

with db.connect() as conn:
    db.add_proposal(conn, ProposalId(1))
    sample_id = db.add_sample(
        conn,
        DBSample(
            id=None,
            proposal_id=ProposalId(1),
            name="sample1",
            attributi=RawAttributiMap({}),
        ),
    )
    db.add_run(
        conn,
        ProposalId(1),
        run_id=9,
        sample_id=sample_id,
        attributi=RawAttributiMap({}),
    )

os.environ[AMARCORD_DB_ENV_VAR] = f"sqlite:///{test_db_filename}?proposal_id=1"
config_path = Path("integration") / "gui" / "crawler.config"
result = ingest_cheetah(config_path)
print(f"Ingested {result.number_of_ingested_data_sources} data source(s)")
result = ingest_cheetah(config_path)
print(f"Ingested {result.number_of_ingested_data_sources} data source(s)")
