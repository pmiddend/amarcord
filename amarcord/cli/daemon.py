import asyncio
import logging
import pickle
from dataclasses import dataclass
from pathlib import Path
from time import time
from typing import Dict
from typing import Optional

import msgpack
import yaml
import zmq
from karabo_bridge import deserialize
from tap import Tap
from zmq.asyncio import Context

from amarcord.amici.onda.zeromq import OnDAZeroMQProcessor
from amarcord.amici.onda.zeromq import TrainRange
from amarcord.amici.xfel.karabo_bridge_slicer import KaraboBridgeSlicer
from amarcord.amici.xfel.karabo_general import ingest_attributi
from amarcord.amici.xfel.karabo_general import ingest_karabo_action
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoTypeDouble
from amarcord.db.constants import ONLINE_SOURCE_NAME
from amarcord.db.db import DB
from amarcord.db.proposal_id import ProposalId
from amarcord.db.tables import create_tables
from amarcord.modules.dbcontext import CreationMode
from amarcord.modules.dbcontext import DBContext
from amarcord.run_id import RunId
from amarcord.train_id import TrainId

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)


@dataclass
class KaraboExport:
    runs: Dict[RunId, TrainRange]


async def karabo_loop(
    db: DB,
    zmq_context: Context,
    proposal_id: ProposalId,
    karabo_config_file: Optional[str],
    karabo_export: KaraboExport,
) -> None:
    if karabo_config_file is None:
        return

    with open(karabo_config_file) as fh:
        karabo_configuration = yaml.load(fh, Loader=yaml.SafeLoader)

    client_endpoint = karabo_configuration["Karabo_bridge"].get("client_endpoint", None)

    if client_endpoint is None:
        return

    # pylint: disable=no-member
    socket = zmq_context.socket(zmq.REQ)

    socket.connect(client_endpoint)

    slicer = KaraboBridgeSlicer(**karabo_configuration["Karabo_bridge"])

    ingest_attributi(db, slicer.get_attributi())

    logger.info("Attributi are set up, waiting for first Karabo data frame")
    debug_counter = 0
    while True:
        # noinspection PyUnresolvedReferences
        await socket.send(b"next")
        try:
            # noinspection PyUnresolvedReferences
            raw_data = await socket.recv_multipart(copy=False)

            logger.debug("karabo: new data received")

            if debug_counter != 0 and debug_counter % 1000 == 0:
                logger.info("still receiving data (counter %s)...", debug_counter)
            debug_counter += 1

            data, metadata = deserialize(raw_data)

            try:
                with db.connect() as conn:
                    with conn.begin():
                        for action in slicer.run_definer(data, metadata):
                            ingest_karabo_action(
                                action, ONLINE_SOURCE_NAME, conn, db, proposal_id
                            )
            except Exception as e:
                error_index = time()
                pickled_fn = Path(f"error-data-frame-{error_index}.pickle")
                pickled_bridge_fn = Path(
                    f"error-data-frame-{error_index}-bridge.pickle"
                )
                logger.exception(
                    "received an exception, dumping pickled data frame to %s",
                    pickled_fn,
                )
                with pickled_fn.open("wb") as error_file:
                    pickle.dump(
                        (data, metadata), error_file, protocol=pickle.HIGHEST_PROTOCOL
                    )
                with pickled_bridge_fn.open("wb") as error_file:
                    pickle.dump(slicer, error_file, protocol=pickle.HIGHEST_PROTOCOL)
                raise e

            for run_id, run_metadata in slicer.run_history.items():
                karabo_export.runs[RunId(run_id)] = TrainRange(
                    TrainId(run_metadata["train_index_initial"].value),
                    TrainId(
                        run_metadata["train_index_initial"].value
                        + run_metadata["trains_in_run"].value
                    ),
                )

            # Do something with data
        except zmq.error.Again:
            logger.error("No data received in time")


async def onda_loop(
    db: DB, zmq_context: Context, onda_url: Optional[str], karabo_export: KaraboExport
) -> None:
    if onda_url is None:
        return

    with db.connect() as conn:
        attributi = db.retrieve_table_attributi(conn, AssociatedTable.RUN)

        if AttributoId("hit_rate") not in attributi:
            db.add_attributo(
                conn,
                "hit_rate",
                "Hit rate",
                AssociatedTable.RUN,
                AttributoTypeDouble(suffix="%"),
            )

    socket = zmq_context.socket(zmq.SUB)
    socket.connect(onda_url)
    # noinspection PyUnresolvedReferences
    socket.setsockopt_string(zmq.SUBSCRIBE, "ondadata")

    processor = OnDAZeroMQProcessor()
    while True:
        logger.debug("Waiting for OnDA data")
        # noinspection PyUnresolvedReferences
        full_msg = await socket.recv_multipart()

        for hit_rate, run_id in processor.process_batch(
            karabo_export.runs, msgpack.unpackb(full_msg[1])
        ):
            write_hit_rate_to_db(db, hit_rate, run_id)

        await asyncio.sleep(1)


def write_hit_rate_to_db(db: DB, result: float, run_id: int) -> None:
    try:
        with db.connect() as conn:
            db.update_run_attributo(
                conn,
                run_id,
                AttributoId("hit_rate"),
                result * 100,
                ONLINE_SOURCE_NAME,
            )
    except Exception as e:
        logger.error(
            "couldn't update hit rate %s for run %s: %s",
            result,
            run_id,
            e,
        )


class Arguments(Tap):
    proposal_id: int  # Proposal ID (integer, no leading digits)
    db_connection_url: str  # Connection URL for the database (e.g. pymysql+mysql://foo/bar)
    karabo_config_file: Optional[
        str
    ] = None  # Karabo configuration file; if given, will enable the Karabo daemon
    onda_url: Optional[
        str
    ] = None  # URL for the OnDA ZeroMQ endpoint; if given, will enable the OnDA client

    """AMARCORD XFEL ingest daemon"""


def main() -> None:
    args = Arguments(underscores_to_dashes=True).parse_args()

    dbcontext = DBContext(args.db_connection_url)

    tables = create_tables(dbcontext)

    if args.db_connection_url.startswith("sqlite://"):
        dbcontext.create_all(creation_mode=CreationMode.CHECK_FIRST)

    db = DB(dbcontext, tables)

    zmq_ctx = Context.instance()

    proposal_id = ProposalId(args.proposal_id)
    if args.db_connection_url.startswith("sqlite://"):
        # Just for testing!
        with db.dbcontext.connect() as local_conn:
            if not db.have_proposals(local_conn):
                db.add_proposal(local_conn, proposal_id)

    asyncio.run(
        async_main(db, proposal_id, args.karabo_config_file, args.onda_url, zmq_ctx)
    )


async def async_main(
    db: DB,
    proposal_id: ProposalId,
    karabo_url: Optional[str],
    onda_url: Optional[str],
    zmq_ctx: Context,
) -> None:
    karabo_export = KaraboExport(runs={})

    await asyncio.gather(
        karabo_loop(db, zmq_ctx, proposal_id, karabo_url, karabo_export),
        onda_loop(db, zmq_ctx, onda_url, karabo_export),
    )


if __name__ == "__main__":
    main()
