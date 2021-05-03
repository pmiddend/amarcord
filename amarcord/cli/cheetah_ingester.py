import logging
import sys
from pathlib import Path
from time import sleep

from tap import Tap

from amarcord.amici.cheetah.analysis import ingest_cheetah
from amarcord.db.db import DB
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.proposal_id import ProposalId
from amarcord.db.tables import create_tables
from amarcord.modules.dbcontext import CreationMode
from amarcord.modules.dbcontext import DBContext

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)


class Arguments(Tap):
    db_connection_url: str  # Connection URL for the database (e.g. pymysql+mysql://foo/bar)
    cheetah_config_path: str  # Path to Cheetah's crawler.config file
    proposal_id: int  # ID of the proposal to ingest data for
    force_run_creation: bool = (
        False  # Create new runs if they don't exist in the DB yet
    )

    """Cheetah output to AMARCORD DB ingester"""


def main() -> int:
    args = Arguments(underscores_to_dashes=True).parse_args()

    dbcontext = DBContext(args.db_connection_url)

    tables = create_tables(dbcontext)
    if args.db_connection_url.startswith("sqlite://"):
        dbcontext.create_all(creation_mode=CreationMode.CHECK_FIRST)
    db = DB(dbcontext, tables)

    if args.db_connection_url.startswith("sqlite://"):
        # Just for testing!
        with db.dbcontext.connect() as local_conn:
            if not db.have_proposals(local_conn):
                db.add_proposal(local_conn, ProposalId(args.proposal_id))

    logger.info("Starting to ingest Cheetah config data")
    while True:
        with db.connect() as conn:
            results = ingest_cheetah(
                Path(args.cheetah_config_path),
                db,
                conn,
                ProposalId(args.proposal_id),
                args.force_run_creation,
            )
            if results.new_data_source_and_run_ids:
                logger.info(
                    "Ingested %s new data source(s) and run id(s): %s",
                    len(results.new_data_source_and_run_ids),
                    ", ".join(
                        f"DS {ds_id}, run {run_id}"
                        for ds_id, run_id in results.new_data_source_and_run_ids
                    ),
                )
                db.add_event(
                    conn,
                    EventLogLevel.INFO,
                    "Cheetah",
                    "Cheetah results for runs: "
                    + ", ".join(
                        str(run_id) for _, run_id in results.new_data_source_and_run_ids
                    ),
                )
            sleep(15)

    # noinspection PyUnreachableCode
    return 0


if __name__ == "__main__":
    sys.exit(main())
