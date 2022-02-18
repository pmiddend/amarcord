import logging
from typing import Any, Union
from typing import Optional

import zmq
from pydantic import BaseModel, ValidationError
from zmq.asyncio import Context

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.attributo_type import AttributoTypeInt


ATTRIBUTO_STOPPED = "stopped"
ATTRIBUTO_NUMBER_OF_HITS = "hits"
ATTRIBUTO_NUMBER_OF_FRAMES = "frames"

logger = logging.getLogger(__name__)


class OmZMQData(BaseModel):
    total_hits: int
    total_frames: int
    timestamp: float


def validate_om_zmq_entry(d: Any) -> Union[str, OmZMQData]:
    if not isinstance(d, dict):
        logger.error("received invalid data from Om: not a dictionary but %s", type(d))
        return f"We expected a dictionary, but got a {type(d)}"

    try:
        return OmZMQData(**d)
    except ValidationError as e:
        return str(e)


class OmZMQProcessor:
    def __init__(self, db: AsyncDB) -> None:
        self._db = db
        self._last_zeromq_data: Optional[OmZMQData] = None

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
                    "om",
                    AssociatedTable.RUN,
                    AttributoTypeInt(),
                )
            if ATTRIBUTO_NUMBER_OF_FRAMES not in attributi:
                await self._db.create_attributo(
                    conn,
                    ATTRIBUTO_NUMBER_OF_FRAMES,
                    "",
                    "om",
                    AssociatedTable.RUN,
                    AttributoTypeInt(),
                )

    async def main_loop(self, zmq_context: Context, url: str, topic: str) -> None:
        logger.info("starting OM observer main loop...")
        socket = zmq_context.socket(zmq.SUB)
        socket.connect(url)
        socket.setsockopt_string(zmq.SUBSCRIBE, topic)

        while True:
            prefix = await socket.recv_string()
            if prefix != topic:
                logger.error(
                    f'Didn\'t receive the topic string "{topic}" before a message, instead "{prefix}". Bailing out since I cannot guarantee proper messaging from this point on.'
                )
                break
            full_msg = await socket.recv_pyobj()
            await self.process_data(full_msg)

    async def process_data(self, data: Any) -> None:
        current_zeromq_data = validate_om_zmq_entry(data)
        if isinstance(current_zeromq_data, str):
            logger.warning(
                f"Got a ZeroMQ frame from OM that's not what we expected. The error is\n\n{current_zeromq_data}\n\nThe data is:\n\n{data}"
            )
            return

        last_zeromq_data = self._last_zeromq_data
        self._last_zeromq_data = current_zeromq_data

        # Special case: if the Om timestamp changed, we cannot rely on the frame difference numbers - ignore this single frame.
        if (
            last_zeromq_data is not None
            and current_zeromq_data is not None
            and last_zeromq_data.timestamp != current_zeromq_data.timestamp
        ):
            logger.info("Detected an Om restart, skipping the initial frame...")
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

            # if we have no previous frame, we cannot build the difference between now and the previous hits, do nothing
            if last_zeromq_data is None:
                return

            # Useless check, but mypy demands it
            assert current_zeromq_data is not None
            new_hits = current_zeromq_data.total_hits - last_zeromq_data.total_hits
            new_frames = (
                current_zeromq_data.total_frames - last_zeromq_data.total_frames
            )
            current_hits = latest_run.attributi.select_int(ATTRIBUTO_NUMBER_OF_HITS)
            current_frames = latest_run.attributi.select_int(ATTRIBUTO_NUMBER_OF_FRAMES)
            latest_run.attributi.append_single(
                ATTRIBUTO_NUMBER_OF_HITS,
                new_hits if current_hits is None else new_hits + current_hits,
            )
            latest_run.attributi.append_single(
                ATTRIBUTO_NUMBER_OF_FRAMES,
                new_frames if current_frames is None else new_frames + current_frames,
            )
            await self._db.update_run_attributi(
                conn, latest_run.id, latest_run.attributi
            )
