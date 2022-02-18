import asyncio
import logging
import time
from random import uniform

import zmq
from zmq.asyncio import Context

logger = logging.getLogger(__name__)


async def om_simulator_loop(zmq_ctx: Context, delay_seconds: float, port: int) -> None:
    publisher = zmq_ctx.socket(zmq.PUB)
    publisher.set_hwm(1)
    publisher.bind(f"tcp://*:{port}")

    total_hits = 0
    total_frames = 0
    timestamp = time.time()

    logger.info("starting om simulator loop...")
    while True:
        await publisher.send_string("view:omdata", zmq.SNDMORE)
        await publisher.send_pyobj(
            {
                "total_hits": total_hits,
                "total_frames": total_frames,
                "timestamp": timestamp,
            }
        )
        new_frames = int(uniform(0, 1000))
        new_hits = int(uniform(0, new_frames))
        total_frames += new_frames
        total_hits += new_hits
        await asyncio.sleep(delay_seconds)
