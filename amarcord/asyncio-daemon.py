import asyncio
from typing import List
from typing import Union
import concurrent
import logging
import zmq

from zmq.asyncio import Context
from karabo_bridge import deserialize

from amarcord.python_schema import validate_dict
from amarcord.config import load_config
from amarcord.modules.dbcontext import DBContext
from amarcord.sources.mc import XFELMetadataCatalogue
from amarcord.sources.mc import XFELMetadataConnectionConfig
from amarcord.sources.karabo import XFELKaraboBridgeConfig

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)


async def karabo_loop(dbctx: DBContext, config: XFELKaraboBridgeConfig) -> None:
    ctx = Context.instance()

    # pylint: disable=no-member
    socket = ctx.socket(zmq.REQ)

    socket.connect(config["server_url"])

    while True:
        await socket.send(b"next")
        try:
            raw_data = await socket.recv_multipart(copy=False)
            _data = deserialize(raw_data)
            logger.info("karabo: new data received")
            await asyncio.sleep(5)
        except zmq.error.Again:
            logger.error("No data received in time")


async def mc_loop(
    dbctx: DBContext,
    executor: concurrent.futures.ThreadPoolExecutor,
    mc_config: XFELMetadataConnectionConfig,
) -> None:
    mc = XFELMetadataCatalogue(mc_config)

    while True:
        logger.info("metadata-client: Retrieving proposal infos...")
        loop = asyncio.get_running_loop()
        infos = await loop.run_in_executor(executor, mc.get_proposal_infos)
        logger.info("metadata-client: Retrieved info about proposals: %s", len(infos))
        await asyncio.sleep(5)


async def main(
    dbcontext: DBContext,
    executor: concurrent.futures.ThreadPoolExecutor,
    mc_config: XFELMetadataConnectionConfig,
    karabo_config: XFELKaraboBridgeConfig,
) -> None:
    #    await asyncio.gather(mc_loop(dbcontext, executor, mc_config))
    await asyncio.gather(
        karabo_loop(dbcontext, karabo_config), mc_loop(dbcontext, executor, mc_config)
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

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as global_executor:
        asyncio.run(
            main(
                global_dbcontext,
                global_executor,
                global_mc_config,
                global_karabo_config,
            )
        )
