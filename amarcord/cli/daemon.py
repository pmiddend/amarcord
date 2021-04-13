import asyncio
import logging
from argparse import ArgumentParser
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import msgpack
import yaml
import zmq
from karabo_bridge import deserialize
from zmq.asyncio import Context

from amarcord.amici.karabo_online import KaraboAttributi  # type: ignore
from amarcord.amici.karabo_online import KaraboBridgeSlicer  # type: ignore
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoType
from amarcord.db.attributo_type import AttributoTypeDateTime
from amarcord.db.attributo_type import AttributoTypeDouble
from amarcord.db.attributo_type import AttributoTypeInt
from amarcord.db.attributo_type import AttributoTypeList
from amarcord.db.attributo_type import AttributoTypeString
from amarcord.db.constants import ONLINE_SOURCE_NAME
from amarcord.db.db import DB
from amarcord.db.db import RunNotFound
from amarcord.db.proposal_id import ProposalId
from amarcord.db.raw_attributi_map import RawAttributiMap
from amarcord.db.tables import create_tables
from amarcord.modules.dbcontext import CreationMode
from amarcord.modules.dbcontext import DBContext
from amarcord.modules.onda.zeromq import OnDAZeroMQProcessor
from amarcord.modules.onda.zeromq import TrainRange
from amarcord.run_id import RunId
from amarcord.util import find_by

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
    "--karabo-config-file",
    required=True,
    help="File for the Karabo Bridge YAML configuration (optional)",
)
parser.add_argument(
    "--onda-url", required=False, help="URL for the OnDA ZeroMQ interface (optional)"
)

args = parser.parse_args()


@dataclass
class KaraboExport:
    runs: Dict[RunId, TrainRange]


def _unit_to_type(type_str: str, unit_str: Optional[str]) -> AttributoType:
    if type_str == "str":
        return AttributoTypeString()
    if type_str == "datetime":
        return AttributoTypeDateTime()
    if type_str == "int":
        return AttributoTypeInt()
    if type_str == "decimal":
        return AttributoTypeDouble(suffix=unit_str)
    if type_str == "unit_type":
        return AttributoTypeDouble(suffix=unit_str, standard_unit=True)
    if type_str.startswith("list["):
        list_type = type_str[5:-1]
        return AttributoTypeList(
            sub_type=_unit_to_type(list_type, unit_str),
            max_length=None,
            min_length=None,
        )
    raise Exception(f"invalid attributo type {type_str} (unit {unit_str})")


def _ingest_attributi(db: DB, attributi: KaraboAttributi) -> None:
    with db.connect() as conn:
        run_attributi = db.retrieve_table_attributi(conn, AssociatedTable.RUN)

        # Remove source from list of attributi (we don't need it)
        unsourced_attributi: List[Dict[str, Any]] = [
            i for k in attributi.values() for i in k
        ]

        attributi_ids = [k["identifier"] for k in unsourced_attributi]

        old_attributi = run_attributi.keys()

        new_attributi_ids = attributi_ids - old_attributi
        not_present_attributi_ids = old_attributi - attributi_ids

        logger.info("new attributi: %s", new_attributi_ids)
        logger.info("attributi in run, not in Karabo: %s", not_present_attributi_ids)

        for n in new_attributi_ids:
            # pylint: disable=cell-var-from-loop
            a = find_by(unsourced_attributi, lambda x: x["identifier"] == n)
            assert a is not None

            db.add_attributo(
                conn,
                n,
                a.get("description", ""),
                AssociatedTable.RUN,
                _unit_to_type(a["type"], a.get("unit", None)),
            )


def _new_run(
    db: DB,
    proposal_id: ProposalId,
    new_run_id: int,
    karabo_attributi: KaraboAttributi,
) -> None:
    with db.connect() as conn:
        db.add_run(
            conn,
            proposal_id,
            new_run_id,
            None,
            _karabo_attributi_to_attributi_map(RawAttributiMap({}), karabo_attributi),
        )


def _karabo_attributi_to_attributi_map(
    attributi: RawAttributiMap, karabo_attributi: KaraboAttributi
) -> RawAttributiMap:
    new_attributi = attributi.copy()
    for a in (i for k in karabo_attributi.values() for i in k):
        new_attributi.append_to_source(ONLINE_SOURCE_NAME, a["value"])
    return new_attributi


def _update_run_per_karabo_attributi(
    db: DB, run_id: int, karabo_attributi: KaraboAttributi
) -> None:
    with db.connect() as conn:
        run = db.retrieve_run(conn, run_id)
        db.update_run_attributi(
            conn,
            run_id,
            _karabo_attributi_to_attributi_map(run.attributi, karabo_attributi),
        )


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

    _ingest_attributi(db, slicer.get_attributi())

    while True:
        # noinspection PyUnresolvedReferences
        await socket.send(b"next")
        try:
            # noinspection PyUnresolvedReferences
            raw_data = await socket.recv_multipart(copy=False)

            logger.info("karabo: new data received")

            data, metadata = deserialize(raw_data)

            with db.connect() as conn:
                with conn.begin():
                    for action in slicer.run_definer(data, metadata):
                        try:
                            run_attributi = db.retrieve_run(
                                conn, action.run_id
                            ).attributi
                            new_attributi = _karabo_attributi_to_attributi_map(
                                run_attributi, action.attributi
                            )
                            db.update_run_attributi(conn, action.run_id, new_attributi)
                        except RunNotFound:
                            run_attributi = _karabo_attributi_to_attributi_map(
                                RawAttributiMap({}), action.attributi
                            )
                            db.add_run(
                                conn,
                                proposal_id,
                                action.run_id,
                                None,
                                run_attributi,
                            )

            for run_id, run_metadata in slicer.run_history.items():
                karabo_export.runs[RunId(run_id)] = TrainRange(
                    run_metadata["train_index_initial"],
                    run_metadata["train_index_initial"] + run_metadata["trains_in_run"],
                )

            # Do something with data
        except zmq.error.Again:
            logger.error("No data received in time")


async def onda_loop(
    db: DB, zmq_context: Context, onda_url: Optional[str], karabo_export: KaraboExport
) -> None:
    if onda_url is None:
        return

    socket = zmq_context.socket(zmq.SUB)
    socket.connect(onda_url)
    # noinspection PyUnresolvedReferences
    socket.setsockopt_string(zmq.SUBSCRIBE, "ondadata")

    processor = OnDAZeroMQProcessor()
    while True:
        logger.info("Waiting for OnDA data")
        # noinspection PyUnresolvedReferences
        full_msg = await socket.recv_multipart()

        for hit_rate, run_id in processor.process_batch(
            karabo_export.runs, msgpack.unpackb(full_msg[1])
        ):
            write_hit_rate_to_db(db, hit_rate, run_id)


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


def main(
    database_url: str,
    proposal_id: ProposalId,
    karabo_config_file: Optional[str],
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

    asyncio.run(async_main(db, proposal_id, karabo_config_file, onda_url, zmq_ctx))


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
    main(
        args.database_url,
        ProposalId(args.proposal_id),
        args.karabo_config_file,
        args.onda_url,
    )
