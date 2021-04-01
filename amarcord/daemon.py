import asyncio
import logging
from argparse import ArgumentParser
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import msgpack
import zmq
from karabo_bridge import deserialize
from zmq.asyncio import Context

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoTypeDouble
from amarcord.db.attributo_type import AttributoTypeInt
from amarcord.db.constants import ONLINE_SOURCE_NAME
from amarcord.db.db import DB
from amarcord.db.db import RunNotFound
from amarcord.db.proposal_id import ProposalId
from amarcord.db.raw_attributi_map import RawAttributiMap
from amarcord.db.tables import create_tables
from amarcord.modules.dbcontext import CreationMode
from amarcord.modules.dbcontext import DBContext

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)

parser = ArgumentParser(description="Daemon to ingest XFEL data from multiple sources")
parser.add_argument(
    "--proposal-id",
    type=int,
    required=True,
    help="Proposal ID (integer, no leading digits)",
)
parser.add_argument("--database-url", required=True, help="Database url")
parser.add_argument(
    "--karabo-url", required=True, help="URL for the Karabo ZeroMQ Bridge (optional)"
)
parser.add_argument(
    "--onda-url", required=False, help="URL for the OnDA ZeroMQ interface (optional)"
)

args = parser.parse_args()


@dataclass
class TrainRange:
    train_begin_inclusive: int
    train_end_inclusive: int


@dataclass
class KaraboExport:
    runs: Dict[int, TrainRange]


def find_run_for_train(karabo_export: KaraboExport, train_id: int) -> Optional[int]:
    return next(
        iter(
            run_id
            for run_id, x in karabo_export.runs.items()
            if x.train_begin_inclusive <= train_id <= x.train_end_inclusive
        ),
        None,
    )


async def karabo_loop(
    db: DB,
    zmq_context: Context,
    proposal_id: ProposalId,
    karabo_url: Optional[str],
    karabo_export: KaraboExport,
) -> None:
    if karabo_url is None:
        return

    # pylint: disable=no-member
    socket = zmq_context.socket(zmq.REQ)

    socket.connect(karabo_url)

    while True:
        # noinspection PyUnresolvedReferences
        await socket.send(b"next")
        try:
            # noinspection PyUnresolvedReferences
            raw_data = await socket.recv_multipart(copy=False)

            logger.info("karabo: new data received")

            data, metadata = deserialize(raw_data)

            # FIXME: Get run ID from data
            run_id = data["run_id"]

            # FIXME: Get train begin/end from data
            karabo_export.runs[run_id].train_begin_inclusive = data["first_train"]
            karabo_export.runs[run_id].train_end_inclusive = data["last_train"]

            with db.connect() as conn:
                with conn.begin():
                    try:
                        run_attributi = db.retrieve_run(conn, run_id).attributi
                    except RunNotFound:
                        run_attributi = RawAttributiMap({})
                        db.add_run(conn, proposal_id, run_id, None, run_attributi)

                    # FIXME: Insert attributi values here!
                    run_attributi.append_single_to_source(
                        ONLINE_SOURCE_NAME, AttributoId("xray_energy"), 3.0
                    )

                    db.update_run_attributi(conn, run_id, run_attributi)

            # Do something with data
        except zmq.error.Again:
            logger.error("No data received in time")


@dataclass(frozen=True)
class OndaZeroMQData:
    event_id: int
    hit_rate: float
    timestamp: float


def validate_onda_entry(d: Any) -> Optional[OndaZeroMQData]:
    if not isinstance(d, dict):
        logger.error(
            "received invalid data from OnDA: not a dictionary but %s", type(d)
        )
        return None

    event_id = d.get("event_id", None)
    if event_id is None:
        logger.error('received invalid data from OnDA, no "event_id": %s', d)
        return None

    hit_rate = d.get("hit_rate", None)
    if hit_rate is None:
        logger.error('received invalid data from OnDA, no "hit_rate": %s', d)
        return None

    timestamp = d.get("timestamp", None)
    if timestamp is None:
        logger.error('received invalid data from OnDA, no "timestamp": %s', d)
        return None

    if not isinstance(event_id, int):
        logger.error(
            'received invalid data from OnDA, "event_id" not integer but %s',
            type(event_id),
        )
        return None

    if not isinstance(timestamp, (int, float)):
        logger.error(
            'received invalid data from OnDA, "timestamp" not number but %s',
            type(timestamp),
        )
        return None

    if not isinstance(hit_rate, (int, float)):
        logger.error(
            'received invalid data from OnDA, "hit_rate" not a number but %s',
            type(hit_rate),
        )
        return None

    return OndaZeroMQData(event_id, hit_rate, timestamp)


def validate_onda_list(d: Any) -> Optional[List[OndaZeroMQData]]:
    if not isinstance(d, list):
        logger.error("received invalid data from OnDA: not a list but %s", type(d))
        return None

    result: List[OndaZeroMQData] = []
    for x in d:
        v = validate_onda_entry(x)
        if v is None:
            return None
        result.append(v)
    return result


async def onda_loop(
    db: DB, zmq_context: Context, onda_url: Optional[str], karabo_export: KaraboExport
) -> None:
    if onda_url is None:
        return

    socket = zmq_context.socket(zmq.SUB)
    socket.connect(onda_url)
    # noinspection PyUnresolvedReferences
    socket.setsockopt_string(zmq.SUBSCRIBE, "ondadata")

    while True:
        logger.info("Waiting for OnDA data")
        # noinspection PyUnresolvedReferences
        full_msg = await socket.recv_multipart()

        onda_entries = validate_onda_list(msgpack.unpackb(full_msg[1]))

        if onda_entries is None:
            continue

        if not onda_entries:
            logger.warning("No entries in OnDA response")
            continue

        logger.info("Valid OnDA data received")

        onda_entry = max(onda_entries, key=lambda entry: entry.timestamp)

        run_for_train = find_run_for_train(karabo_export, onda_entry.event_id)

        if run_for_train is None:
            logger.warning("couldn't find corresponding run for train ID %s")
            continue

        try:
            with db.connect() as conn:
                db.update_run_attributo(
                    conn,
                    run_for_train,
                    AttributoId("hit_rate"),
                    onda_entry.hit_rate * 100,
                    ONLINE_SOURCE_NAME,
                )
        except Exception as e:
            logger.error(
                "couldn't update hit rate %s for run %s: %s",
                onda_entry.hit_rate,
                run_for_train,
                e,
            )


def mymain(
    database_url: str,
    proposal_id: ProposalId,
    karabo_url: Optional[str],
    onda_url: Optional[str],
) -> None:
    dbcontext = DBContext(database_url)

    tables = create_tables(dbcontext)

    dbcontext.create_all(creation_mode=CreationMode.CHECK_FIRST)

    db = DB(dbcontext, tables)

    zmq_ctx = Context.instance()

    # Just for testing!
    with db.dbcontext.connect() as local_conn:
        db.add_proposal(local_conn, proposal_id)

        # FIXME: Add other attributo here
        db.add_attributo(
            local_conn, "xray_energy", "", AssociatedTable.RUN, AttributoTypeDouble()
        )
        db.add_attributo(
            local_conn, "hit_rate", "", AssociatedTable.RUN, AttributoTypeDouble()
        )
        db.add_attributo(
            local_conn, "first_train", "", AssociatedTable.RUN, AttributoTypeInt()
        )
        db.add_attributo(
            local_conn, "last_train", "", AssociatedTable.RUN, AttributoTypeInt()
        )

    asyncio.run(async_main(db, proposal_id, karabo_url, onda_url, zmq_ctx))


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
    mymain(
        args.database_url, ProposalId(args.proposal_id), args.karabo_url, args.onda_url
    )
