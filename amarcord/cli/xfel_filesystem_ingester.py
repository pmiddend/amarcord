import argparse
import logging
from typing import Optional

import numpy as np

from amarcord.amici.xfel.karabo_action import KaraboRunStart
from amarcord.amici.xfel.karabo_general import ingest_attributi
from amarcord.amici.xfel.karabo_general import ingest_karabo_action
from amarcord.amici.xfel.karabo_online import load_configuration
from amarcord.amici.xfel.xfel_filesystem import FileSystem2Attributo
from amarcord.db.constants import OFFLINE_SOURCE_NAME
from amarcord.db.db import DB
from amarcord.db.proposal_id import ProposalId
from amarcord.db.tables import create_tables
from amarcord.modules.dbcontext import CreationMode
from amarcord.modules.dbcontext import DBContext

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d %(levelname)8s [%(module)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG,
)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Read data offline, compute statistics and send results to AMARCORD."
    )
    parser.add_argument(
        "--proposal-id",
        metavar="proposalId",
        type=int,
        help="Proposal ID",
    )
    parser.add_argument("--run-id", metavar="RunId", type=int, help="Run ID")
    parser.add_argument(
        "--config",
        metavar="YAML",
        default="config.yml",
        help="Config file (default: %(default)s)",
    )
    parser.add_argument("--database-url", metavar="URL", help="Database url")
    parser.add_argument(
        "--number-of-bunches",
        metavar="N",
        type=int,
        default=0,
        help="Number of bunches, of not available",
    )

    args = parser.parse_args()

    config = load_configuration(args.config)
    data = FileSystem2Attributo(
        args.proposal_id, args.run_id, **config["Karabo_bridge"]
    )
    data.extract_data(args.number_of_bunches)
    attributi_definition = data.compute_statistics()

    logging.info("\n\nReduced values:")
    for group in data.attributi:
        for attributo in data.attributi[group].values():
            if attributo.value is None or attributo.source not in data.cache:
                continue

            cached_value = data.cache[attributo.source][attributo.key]
            if not isinstance(cached_value, np.ndarray):
                continue

            original_shape: Optional[np.ndarray]
            try:
                original_shape = cached_value.shape  # type: ignore
            except:
                original_shape = None

            logging.info(
                "%s (original shape %s): %s %s [%s]",
                attributo.identifier,
                original_shape,
                attributo.value,
                attributo.unit if attributo.unit is not None else "",
                attributo.type_,
            )

    if args.database_url:
        dbcontext = DBContext(args.database_url, echo=True)

        tables = create_tables(dbcontext)

        if args.database_url.startswith("sqlite://"):
            dbcontext.create_all(creation_mode=CreationMode.CHECK_FIRST)

        db = DB(dbcontext, tables)

        with db.connect() as conn:
            if args.database_url.startswith("sqlite://"):
                if not db.have_proposals(conn):
                    db.add_proposal(conn, ProposalId(args.proposal_id))

            ingest_attributi(db, data.expected_attributi)

            ingest_karabo_action(
                KaraboRunStart(args.run_id, args.proposal_id, data.attributi),
                OFFLINE_SOURCE_NAME,
                conn,
                db,
                ProposalId(args.proposal_id),
            )
