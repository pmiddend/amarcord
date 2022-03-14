import asyncio
import logging
import pickle
from pathlib import Path
from time import time
from typing import Optional

import yaml
import zmq
from karabo_bridge import deserialize
from tap import Tap
from zmq.asyncio import Context

from amarcord.amici.xfel.karabo_bridge import (
    Karabo2,
    parse_configuration,
    KaraboConfigurationError,
    ingest_bridge_output,
)
from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.dbcontext import CreationMode
from amarcord.db.tables import create_tables_from_metadata

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)


async def karabo_loop(
    db: AsyncDB,
    zmq_context: Context,
    karabo_endpoint: Optional[str],
    karabo_config_file: Optional[Path],
) -> None:
    if karabo_config_file is None or karabo_endpoint is None:
        return

    socket = zmq_context.socket(zmq.REQ)  # type: ignore

    socket.connect(karabo_endpoint)

    with karabo_config_file.open("r") as f:
        config_file = parse_configuration(yaml.load(f, Loader=yaml.SafeLoader))
        if isinstance(config_file, KaraboConfigurationError):
            raise Exception(config_file)
        karabo2 = Karabo2(config_file)

    async with db.begin() as conn:
        await karabo2.create_missing_attributi(db, conn)

    logger.info("attributi are set up, waiting for first Karabo data frame")
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
                result = karabo2.process_frame(metadata, data)

                if result is None:
                    continue

                async with db.begin() as conn:
                    await ingest_bridge_output(db, conn, result)
            except Exception as e:
                error_index = time()
                pickled_fn = Path(f"error-data-frame-{error_index}.pickle")
                logger.exception(
                    "received an exception, dumping pickled data frame to %s",
                    pickled_fn,
                )
                with pickled_fn.open("wb") as error_file:
                    pickle.dump(
                        (data, metadata), error_file, protocol=pickle.HIGHEST_PROTOCOL
                    )
                raise e

            # Do something with data
        except zmq.error.Again:
            logger.error("No data received in time")


# async def onda_loop(
#     db: DB, zmq_context: Context, onda_url: Optional[str], karabo_export: KaraboExport
# ) -> None:
#     if onda_url is None:
#         return
#
#     with db.connect() as conn:
#         attributi = db.retrieve_table_attributi(conn, AssociatedTable.RUN)
#
#         if AttributoId("hit_rate") not in attributi:
#             db.add_attributo(
#                 conn,
#                 "hit_rate",
#                 "Hit rate",
#                 AssociatedTable.RUN,
#                 AttributoTypeDouble(suffix="%"),
#             )
#
#     socket = zmq_context.socket(zmq.SUB)
#     socket.connect(onda_url)
#     # noinspection PyUnresolvedReferences
#     socket.setsockopt_string(zmq.SUBSCRIBE, "ondadata")
#
#     processor = OnDAZeroMQProcessor()
#     while True:
#         logger.debug("Waiting for OnDA data")
#         # noinspection PyUnresolvedReferences
#         full_msg = await socket.recv_multipart()
#
#         for hit_rate, run_id in processor.process_batch(
#             karabo_export.runs, msgpack.unpackb(full_msg[1])
#         ):
#             write_hit_rate_to_db(db, hit_rate, run_id)
#
#         await asyncio.sleep(1)
#
#
# def write_hit_rate_to_db(db: DB, result: float, run_id: int) -> None:
#     try:
#         with db.connect() as conn:
#             db.update_run_attributo(
#                 conn,
#                 run_id,
#                 AttributoId("hit_rate"),
#                 result * 100,
#                 ONLINE_SOURCE_NAME,
#             )
#     except Exception as e:
#         logger.error(
#             "couldn't update hit rate %s for run %s: %s",
#             result,
#             run_id,
#             e,
#         )


class Arguments(Tap):
    db_connection_url: str  # Connection URL for the database (e.g. pymysql+mysql://foo/bar)
    karabo_config_file: Optional[
        Path
    ] = None  # Karabo configuration file; if given, will enable the Karabo daemon
    karabo_endpoint: Optional[
        str
    ] = None  # Endpoint for the Karabo bridge (for example, tcp://localhost:4545)
    onda_url: Optional[
        str
    ] = None  # URL for the OnDA ZeroMQ endpoint; if given, will enable the OnDA client

    """AMARCORD XFEL ingest daemon"""


def main() -> None:
    args = Arguments(underscores_to_dashes=True).parse_args()

    dbcontext = AsyncDBContext(args.db_connection_url)

    tables = create_tables_from_metadata(dbcontext.metadata)

    if args.db_connection_url.startswith("sqlite+aiosqlite://"):
        asyncio.run(dbcontext.create_all(creation_mode=CreationMode.CHECK_FIRST))

    db = AsyncDB(dbcontext, tables)

    zmq_ctx = Context.instance()

    asyncio.run(
        async_main(
            db,
            args.karabo_endpoint,
            args.karabo_config_file,
            zmq_ctx,
        )
    )


async def async_main(
    db: AsyncDB,
    karabo_endpoint: Optional[str],
    karabo_config_file: Optional[Path],
    # onda_url: Optional[str],
    zmq_ctx: Context,
) -> None:
    await karabo_loop(db, zmq_ctx, karabo_endpoint, karabo_config_file)
    # await asyncio.gather(
    #     karabo_loop(
    #         db, zmq_ctx, proposal_id, karabo_endpoint, karabo_config_file, karabo_export
    #     ),
    #     onda_loop(db, zmq_ctx, onda_url, karabo_export),
    # )


if __name__ == "__main__":
    main()
