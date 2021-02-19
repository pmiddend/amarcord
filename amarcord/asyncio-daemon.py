import asyncio
from typing import List
from typing import Dict
from typing import Any
from typing import Union
import concurrent
import logging
import pickle

import zmq
import sqlalchemy as sa
from zmq.asyncio import Context
from karabo_bridge import deserialize

from amarcord.python_schema import validate_dict
from amarcord.config import load_config
from amarcord.modules.dbcontext import DBContext
from amarcord.sources.mc import XFELMetadataCatalogue
from amarcord.sources.mc import XFELMetadataConnectionConfig
from amarcord.sources.karabo import XFELKaraboBridgeConfig
from amarcord.modules.dbcontext import CreationMode
from amarcord.modules.spb.tables import create_tables, Tables
from amarcord.modules.spb.run_id import RunId
from amarcord.modules.spb.proposal_id import ProposalId
from amarcord.modules.spb.queries import SPBQueries
from amarcord.modules.spb.tables import create_tables

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)


async def karabo_loop(queries: SPBQueries, config: XFELKaraboBridgeConfig) -> None:
    # TESTING
    with queries.dbcontext.connect() as conn:
        queries.create_run(conn, ProposalId(1), RunId(1), sample_id=None)

    ctx = Context.instance()

    # pylint: disable=no-member
    socket = ctx.socket(zmq.REQ)

    socket.connect(config["socket_url"])

    while True:
        await socket.send(b"next")
        try:
            raw_data = await socket.recv_multipart(copy=False)
            data = deserialize(raw_data)

            # with open("/tmp/pickled_karabo", "wb") as f:
            #     f.write(pickle.dumps(data))

            with queries.dbcontext.connect() as conn:
                queries.update_run(conn, RunId(1), karabo=pickle.dumps(data))

            logger.info("karabo: new data received")

            await asyncio.sleep(5)
        except zmq.error.Again:
            logger.error("No data received in time")


def _update_db_from_mc(dbctx: DBContext, tables: Tables, infos: Dict[int, Any]) -> None:
    with dbctx.connect() as conn:
        with conn.begin():
            existing_props = set(
                row[0]
                for row in conn.execute(sa.select([tables.proposal.c.id])).fetchall()
            )
            for prop_id, info in infos.items():
                data = info["data"]
                if prop_id in existing_props:
                    conn.execute(
                        sa.update(tables.proposal)
                        .where(tables.proposal.c.id == prop_id)
                        .values(metadata=data)
                    )
                else:
                    conn.execute(
                        tables.proposal.insert().values(id=prop_id, metadata=data)
                    )


async def mc_loop(
    dbctx: DBContext,
    tables: Tables,
    executor: concurrent.futures.ThreadPoolExecutor,
    mc_config: XFELMetadataConnectionConfig,
) -> None:
    mc = XFELMetadataCatalogue(mc_config)

    while True:
        logger.info("metadata-client: Retrieving proposal infos...")
        loop = asyncio.get_running_loop()
        infos = await loop.run_in_executor(executor, mc.get_proposal_infos)
        logger.info("metadata-client: Retrieved info about proposals: %s", len(infos))
        await loop.run_in_executor(executor, _update_db_from_mc, dbctx, tables, infos)
        await asyncio.sleep(5)


async def main(
    queries: SPBQueries,
    executor: concurrent.futures.ThreadPoolExecutor,
    mc_config: XFELMetadataConnectionConfig,
    karabo_config: XFELKaraboBridgeConfig,
) -> None:
    await asyncio.gather(
        karabo_loop(queries, karabo_config),
        # mc_loop(dbcontext, tables, executor, mc_config),
    )


if __name__ == "__main__":
    c = load_config()

    global_mc_config: Union[List[str], XFELMetadataConnectionConfig] = validate_dict(
        c["mc"], XFELMetadataConnectionConfig
    )
    if isinstance(global_mc_config, list):
        raise Exception(f"Metadata configuration invalid: {global_mc_config}")

    global_karabo_config: Union[List[str], XFELKaraboBridgeConfig] = validate_dict(
        c["karabo"], XFELKaraboBridgeConfig
    )
    if isinstance(global_karabo_config, list):
        raise Exception(f"Karabo configuration invalid: {global_karabo_config}")

    global_dbcontext = DBContext(c["db"]["url"])

    tables = create_tables(global_dbcontext)
    global_dbcontext.create_all(creation_mode=CreationMode.CHECK_FIRST)

    global_queries = SPBQueries(global_dbcontext, tables)

    # Just for testing!
    with global_queries.dbcontext.connect() as conn:
        global_queries.create_proposal(conn, ProposalId(1))

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as global_executor:
        asyncio.run(
            main(
                global_queries,
                global_executor,
                global_mc_config,
                global_karabo_config,
            )
        )
