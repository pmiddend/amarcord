import argparse
import logging
import pickle
from pathlib import Path

from amarcord.amici.xfel.karabo_bridge_slicer import KaraboBridgeSlicer
from amarcord.amici.xfel.karabo_general import ingest_karabo_action
from amarcord.amici.xfel.karabo_online import load_configuration
from amarcord.db.constants import ONLINE_SOURCE_NAME
from amarcord.db.db import DB
from amarcord.db.proposal_id import ProposalId
from amarcord.db.tables import create_tables
from amarcord.modules.dbcontext import CreationMode
from amarcord.modules.dbcontext import DBContext
from amarcord.util import natural_key

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d %(levelname)8s [%(module)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Read pickled Karabo client frames and ingest them into a database"
    )
    parser.add_argument(
        "--dump-path",
        help="Path with the dump files",
        required=True,
    )
    parser.add_argument(
        "--karabo-configuration",
        help="Karabo configuration file",
        default="./config.yml",
        required=True,
    )
    parser.add_argument(
        "--database-url", type=str, help="URL to the database", required=True
    )

    args = parser.parse_args()

    config = load_configuration(args.karabo_configuration)

    dbcontext = DBContext(args.database_url)

    tables = create_tables(dbcontext)

    if args.database_url.startswith("sqlite://"):
        dbcontext.create_all(creation_mode=CreationMode.CHECK_FIRST)

    db = DB(dbcontext, tables)

    with db.connect() as conn:
        if args.database_url.startswith("sqlite://") and not db.have_proposals(conn):
            db.add_proposal(conn, ProposalId(1))

    karabo_data = KaraboBridgeSlicer(**config["Karabo_bridge"])

    files = list(Path(args.dump_path).iterdir())

    if not files:
        logger.warning("No files matching glob %s", args.file_glob)
        return

    i = 0
    for fn in sorted(files, key=lambda x: natural_key(x.name)):
        if i % 1000 == 0:
            logger.info("still ingesting, current frame %s", fn)
        i += 1

        with fn.open("rb") as f:
            data, metadata = pickle.load(f)

            for action in karabo_data.run_definer(data, metadata):
                logger.info("%s, Karabo action type %s", fn, type(action))
                logger.debug("action data: %s", action)

                with db.connect() as conn:
                    ingest_karabo_action(
                        action, ONLINE_SOURCE_NAME, conn, db, ProposalId(1)
                    )

    logger.info("finished ingesting")


if __name__ == "__main__":
    main()
