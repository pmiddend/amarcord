import asyncio
import logging
import time
from random import uniform

import zmq
from tap import Tap
from zmq.asyncio import Context

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)


class Arguments(Tap):
    port: int


async def main_loop():
    args = Arguments(underscores_to_dashes=True).parse_args()

    zmq_ctx = Context.instance()
    publisher = zmq_ctx.socket(zmq.PUB)
    publisher.set_hwm(1)
    publisher.bind(f"tcp://*:{args.port}")

    total_hits = 0
    total_frames = 0
    timestamp = time.time()

    while True:
        logger.info("sending package")
        await publisher.send_string("view:omdata", zmq.SNDMORE)
        await publisher.send_pyobj(
            {
                "total_hits": total_hits,
                "total_frames": total_frames,
                "timestamp": timestamp,
            }
        )
        new_frames = uniform(0, 1000)
        new_hits = uniform(0, new_frames)
        total_frames += new_frames
        total_hits += new_hits
        await asyncio.sleep(5.0)


asyncio.run(main_loop())
