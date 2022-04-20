import logging
from dataclasses import dataclass
from typing import Any, Union

import msgpack
import zmq
from zmq.asyncio import Context

from amarcord.amici.om.client import (
    ATTRIBUTO_NUMBER_OF_HITS,
    ATTRIBUTO_NUMBER_OF_FRAMES,
)
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.attributi import ATTRIBUTO_STOPPED
from amarcord.db.attributo_type import AttributoTypeInt

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OnDAZMQData:
    frames: int
    hits: int


def validate_onda_zmq_entry(d: Any) -> Union[str, OnDAZMQData]:
    if not isinstance(d, list):
        logger.error("received invalid data from OnDA: not a list but %s", type(d))
        return f"We expected a list, but got a {type(d)}"

    return OnDAZMQData(
        frames=len(d), hits=len([x for x in d if x.get("frame_is_hit", False)])
    )


class OnDAZMQProcessor:
    def __init__(self, db: AsyncDB) -> None:
        self._db = db

    async def init(self) -> None:
        async with self._db.begin() as conn:
            attributi = {
                k.name: k
                for k in await self._db.retrieve_attributi(
                    conn, associated_table=AssociatedTable.RUN
                )
            }
            if ATTRIBUTO_NUMBER_OF_HITS not in attributi:
                await self._db.create_attributo(
                    conn,
                    ATTRIBUTO_NUMBER_OF_HITS,
                    "",
                    "onda",
                    AssociatedTable.RUN,
                    AttributoTypeInt(),
                )
            if ATTRIBUTO_NUMBER_OF_FRAMES not in attributi:
                await self._db.create_attributo(
                    conn,
                    ATTRIBUTO_NUMBER_OF_FRAMES,
                    "",
                    "onda",
                    AssociatedTable.RUN,
                    AttributoTypeInt(),
                )

    async def main_loop(self, zmq_context: Context, url: str, topic: str) -> None:
        logger.info("starting OnDA observer main loop...")
        socket = zmq_context.socket(zmq.SUB)  # type: ignore
        socket.connect(url)
        socket.setsockopt_string(zmq.SUBSCRIBE, topic)

        while True:
            prefix = await socket.recv_string()
            if prefix != topic:
                logger.error(
                    f'Didn\'t receive the topic string "{topic}" before a message, instead "{prefix}". Bailing out since I cannot guarantee proper messaging from this point on.'
                )
                break
            # noinspection PyUnresolvedReferences
            full_msg = await socket.recv()
            await self.process_data(msgpack.unpackb(full_msg))

    async def process_data(self, data: Any) -> None:
        current_zeromq_data = validate_onda_zmq_entry(data)
        if isinstance(current_zeromq_data, str):
            logger.warning(
                f"Got a ZeroMQ frame from OnDA that's not what we expected. The error is\n\n{current_zeromq_data}\n\nThe data is:\n\n{data}"
            )
            return

        async with self._db.begin() as conn:
            attributi = await self._db.retrieve_attributi(conn, associated_table=None)

            # we have no "stopped" attributo? we can't really work like this!
            if not any(x for x in attributi if x.name == ATTRIBUTO_STOPPED):
                return

            latest_run = await self._db.retrieve_latest_run(conn, attributi)
            # No run, nothing to do
            if latest_run is None:
                return

            # Run is...running! Let's update the hit rate if we have a previous frame
            if latest_run.attributi.select_datetime(ATTRIBUTO_STOPPED) is not None:
                return

            # Useless check, but mypy demands it
            assert current_zeromq_data is not None
            new_hits = current_zeromq_data.hits
            new_frames = current_zeromq_data.frames
            current_hits = latest_run.attributi.select_int(ATTRIBUTO_NUMBER_OF_HITS)
            current_frames = latest_run.attributi.select_int(ATTRIBUTO_NUMBER_OF_FRAMES)
            final_om_hits = (
                new_hits if current_hits is None else new_hits + current_hits
            )
            latest_run.attributi.append_single(
                ATTRIBUTO_NUMBER_OF_HITS,
                final_om_hits,
            )
            final_om_frames = (
                new_frames if current_frames is None else new_frames + current_frames
            )
            latest_run.attributi.append_single(
                ATTRIBUTO_NUMBER_OF_FRAMES,
                final_om_frames,
            )
            await self._db.update_run_attributi(
                conn, latest_run.id, latest_run.attributi
            )
