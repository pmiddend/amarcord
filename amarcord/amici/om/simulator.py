import asyncio
import time
from random import uniform

import structlog
import zmq
from zmq.asyncio import Context

logger = structlog.stdlib.get_logger(__name__)


async def om_simulator_loop(zmq_ctx: Context, delay_seconds: float, port: int) -> None:
    publisher = zmq_ctx.socket(zmq.PUB)  # type: ignore
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
                "num_hits": total_hits,
                "num_events": total_frames,
                "start_timestamp": timestamp,
            }
        )
        new_frames = int(uniform(0, 1000))
        new_hits = int(uniform(0, 3) / 100.0 * new_frames)
        total_frames += new_frames
        total_hits += new_hits
        await asyncio.sleep(delay_seconds)
