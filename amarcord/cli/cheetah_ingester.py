import argparse
import logging
import sys
from time import sleep

from amarcord.amici.analysis import ingest_cheetah_internal
from amarcord.db.db import DB
from amarcord.db.tables import create_tables
from amarcord.modules.dbcontext import CreationMode
from amarcord.modules.dbcontext import DBContext

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(prog="AMARCORD Cheetah ingester")
    parser.add_argument(
        "--connection-url",
        type=str,
        required=True,
        help="Connection URL to the database",
    )
    parser.add_argument(
        "--cheetah-config-path",
        type=str,
        required=True,
        help="Path to Cheetah's crawler.config file",
    )

    args = parser.parse_args()

    dbcontext = DBContext(args.connection_url)

    tables = create_tables(dbcontext)
    if args.database_url.startswith("sqlite://"):
        dbcontext.create_all(creation_mode=CreationMode.CHECK_FIRST)
    db = DB(dbcontext, tables)

    if args.database_url.startswith("sqlite://"):
        # Just for testing!
        with db.dbcontext.connect() as local_conn:
            if db.have_proposals(local_conn):
                db.add_proposal(local_conn, args.proposal_id)

    logger.info("Starting to ingest Cheetah config data")
    while True:
        with db.connect() as conn:
            results = ingest_cheetah_internal(args.cheetah_config_path, db, conn)
            if results.number_of_ingested_data_sources:
                logger.info(
                    "Ingested %s new data source(s)",
                    results.number_of_ingested_data_sources,
                )
            sleep(15)

    # noinspection PyUnreachableCode
    return 0


if __name__ == "__main__":
    sys.exit(main())
