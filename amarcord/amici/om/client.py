import datetime
import logging
from typing import Any
from typing import Final

import zmq
from pint import UnitRegistry
from pydantic import BaseModel
from pydantic import ValidationError
from zmq.asyncio import Context

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.async_dbcontext import Connection
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.attributi import ATTRIBUTO_STARTED
from amarcord.db.attributi import ATTRIBUTO_STOPPED
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoTypeDecimal
from amarcord.db.attributo_type import AttributoTypeInt
from amarcord.db.dbattributo import DBAttributo

OM_ATTRIBUTO_GROUP: Final = "om"

ATTRIBUTO_NUMBER_OF_HITS: Final = AttributoId("hits")
ATTRIBUTO_NUMBER_OF_FRAMES: Final = AttributoId("frames")
ATTRIBUTO_NUMBER_OF_OM_HITS: Final = AttributoId("om_hits")
ATTRIBUTO_NUMBER_OF_OM_FRAMES: Final = AttributoId("om_frames")
ATTRIBUTO_FRAME_TIME: Final = AttributoId("frame_time")

logger = logging.getLogger(__name__)


class OmZMQData(BaseModel):
    num_hits: int
    num_events: int
    start_timestamp: float


def validate_om_zmq_entry(d: Any) -> str | OmZMQData:
    if not isinstance(d, dict):
        logger.error("received invalid data from Om: not a dictionary but %s", type(d))
        return f"We expected a dictionary, but got a {type(d)}"

    try:
        return OmZMQData(**d)
    except ValidationError as e:
        return str(e)


async def create_om_attributi(
    db: AsyncDB, conn: Connection, attributi_list: list[DBAttributo]
) -> None:
    attributi = {k.name: k for k in attributi_list}
    if ATTRIBUTO_NUMBER_OF_HITS not in attributi:
        await db.create_attributo(
            conn,
            ATTRIBUTO_NUMBER_OF_HITS,
            "",
            OM_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            AttributoTypeInt(),
        )
    if ATTRIBUTO_NUMBER_OF_FRAMES not in attributi:
        await db.create_attributo(
            conn,
            ATTRIBUTO_NUMBER_OF_FRAMES,
            "",
            OM_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            AttributoTypeInt(),
        )
    if ATTRIBUTO_NUMBER_OF_OM_FRAMES not in attributi:
        await db.create_attributo(
            conn,
            ATTRIBUTO_NUMBER_OF_OM_FRAMES,
            "",
            OM_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            AttributoTypeInt(),
        )
    if ATTRIBUTO_NUMBER_OF_OM_HITS not in attributi:
        await db.create_attributo(
            conn,
            ATTRIBUTO_NUMBER_OF_OM_HITS,
            "",
            OM_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            AttributoTypeInt(),
        )


class OmZMQProcessor:
    def __init__(self, db: AsyncDB) -> None:
        self._db = db
        self._last_zeromq_data: OmZMQData | None = None

    async def init(self) -> None:
        async with self._db.begin() as conn:
            await create_om_attributi(
                self._db,
                conn,
                await self._db.retrieve_attributi(conn, AssociatedTable.RUN),
            )

    async def main_loop(self, zmq_context: Context, url: str, topic: str) -> None:
        logger.info("starting OM observer main loop...")
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
            and last_zeromq_data.start_timestamp != current_zeromq_data.start_timestamp
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
            new_hits = current_zeromq_data.num_hits - last_zeromq_data.num_hits
            new_frames = current_zeromq_data.num_events - last_zeromq_data.num_events
            current_hits = latest_run.attributi.select_int(ATTRIBUTO_NUMBER_OF_OM_HITS)
            current_frames = latest_run.attributi.select_int(
                ATTRIBUTO_NUMBER_OF_OM_FRAMES
            )
            final_om_hits = (
                new_hits if current_hits is None else new_hits + current_hits
            )
            latest_run.attributi.append_single(
                ATTRIBUTO_NUMBER_OF_OM_HITS,
                final_om_hits,
            )
            final_om_frames = (
                new_frames if current_frames is None else new_frames + current_frames
            )
            latest_run.attributi.append_single(
                ATTRIBUTO_NUMBER_OF_OM_FRAMES,
                final_om_frames,
            )
            # If we have an exposure time, we can even update the total number of hits/frames
            frame_time = latest_run.attributi.select_decimal(ATTRIBUTO_FRAME_TIME)
            started = latest_run.attributi.select_datetime(ATTRIBUTO_STARTED)
            if (
                frame_time is not None
                and frame_time > 0
                and started is not None
                and final_om_frames > 0
            ):
                frame_time_attributo = next(
                    iter(x for x in attributi if x.name == ATTRIBUTO_FRAME_TIME), None
                )
                if frame_time_attributo is None:
                    logger.error(
                        f'frame time attributo "{ATTRIBUTO_FRAME_TIME}" missing in attributo list'
                    )
                    return

                if not isinstance(
                    frame_time_attributo.attributo_type, AttributoTypeDecimal
                ):
                    logger.error(
                        f'frame time attributo "{ATTRIBUTO_FRAME_TIME}" is not decimal but {frame_time_attributo.attributo_type}'
                    )

                if not frame_time_attributo.attributo_type.standard_unit:
                    logger.error(
                        f"frame time attributo {ATTRIBUTO_FRAME_TIME} is a decimal, but not a standard unit: {frame_time_attributo.attributo_type}"
                    )
                    return

                frame_time_quantity = (
                    UnitRegistry()(frame_time_attributo.attributo_type.suffix)
                    * frame_time
                ).to("s")
                seconds_since_start = (
                    datetime.datetime.utcnow() - started
                ) / datetime.timedelta(seconds=1)
                frames_since_start = seconds_since_start / frame_time_quantity.m
                hits_since_start = final_om_hits / final_om_frames * frames_since_start
                latest_run.attributi.append_single(
                    ATTRIBUTO_NUMBER_OF_FRAMES,
                    int(frames_since_start),
                )
                latest_run.attributi.append_single(
                    ATTRIBUTO_NUMBER_OF_HITS,
                    int(hits_since_start),
                )

            await self._db.update_run_attributi(
                conn, latest_run.id, latest_run.attributi
            )
