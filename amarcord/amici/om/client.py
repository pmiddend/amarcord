import datetime
from pathlib import Path
from typing import Any
from typing import Final

import structlog
import zmq
from pydantic import BaseModel
from pydantic import ValidationError
from structlog.stdlib import BoundLogger
from zmq.asyncio import Context

from amarcord.db.asyncdb import AsyncDB
from amarcord.db.attributi import ATTRIBUTO_STOPPED
from amarcord.db.indexing_result import DBIndexingFOM
from amarcord.db.indexing_result import DBIndexingResultInput
from amarcord.db.indexing_result import DBIndexingResultRunning
from amarcord.db.indexing_result import DBIndexingResultRuntimeStatus
from amarcord.db.indexing_result import empty_indexing_fom

_DUMMY_JOB_ID: Final = 0

_DUMMY_STREAM: Final = Path("dummy.stream")

OM_ATTRIBUTO_GROUP: Final = "om"

logger = structlog.stdlib.get_logger(__name__)


class OmZMQData(BaseModel):
    num_hits: int
    num_events: int
    start_timestamp: float


def validate_om_zmq_entry(log: BoundLogger, d: Any) -> str | OmZMQData:
    if not isinstance(d, dict):
        log.error("received invalid data from Om: not a dictionary but %s", type(d))
        return f"We expected a dictionary, but got a {type(d)}"

    try:
        return OmZMQData(**d)
    except ValidationError as e:
        return str(e)


class OmZMQProcessor:
    def __init__(self, db: AsyncDB) -> None:
        self._db = db
        self._last_zeromq_data: OmZMQData | None = None
        self._log = logger

    async def main_loop(self, zmq_context: Context, url: str, topic: str) -> None:
        self._log = self._log.bind(url=url, topic=topic)
        self._log.info("starting OM observer main loop...")
        socket = zmq_context.socket(zmq.SUB)  # type: ignore
        socket.connect(url)
        socket.setsockopt_string(zmq.SUBSCRIBE, topic)

        while True:
            prefix = await socket.recv_string()
            if prefix != topic:
                self._log.error(
                    f'Didn\'t receive the topic string before a message, instead "{prefix}". Bailing out since I cannot guarantee proper messaging from this point on.'
                )
                break
            full_msg = await socket.recv_pyobj()
            await self.process_data(full_msg)

    async def process_data(self, data: Any) -> None:
        current_zeromq_data = validate_om_zmq_entry(self._log, data)
        if isinstance(current_zeromq_data, str):
            self._log.warning(
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
            self._log.info("Detected an Om restart, skipping the initial frame...")
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

            latest_indexing_result = await self._db.retrieve_indexing_result_for_run(
                conn, latest_run.id
            )
            current_hits: None | int
            current_frames: None | int
            not_indexed_frames: None | int

            latest_runtime_status: DBIndexingResultRuntimeStatus
            if latest_indexing_result is None:
                latest_runtime_status = DBIndexingResultRunning(
                    stream_file=_DUMMY_STREAM,
                    job_id=_DUMMY_JOB_ID,
                    fom=empty_indexing_fom,
                )
                current_hits = 0
                current_frames = 0
                not_indexed_frames = 0
            else:
                current_hits = latest_indexing_result.hits
                current_frames = latest_indexing_result.frames
                not_indexed_frames = latest_indexing_result.not_indexed_frames

                if latest_indexing_result.runtime_status is None:
                    latest_runtime_status = DBIndexingResultRunning(
                        stream_file=_DUMMY_STREAM,
                        job_id=_DUMMY_JOB_ID,
                        fom=empty_indexing_fom,
                    )
                else:
                    latest_runtime_status = latest_indexing_result.runtime_status

            # Useless check, but mypy demands it
            assert current_zeromq_data is not None
            new_hits = current_zeromq_data.num_hits - last_zeromq_data.num_hits
            new_frames = current_zeromq_data.num_events - last_zeromq_data.num_events
            final_om_hits = (
                new_hits if current_hits is None else new_hits + current_hits
            )
            final_om_frames = (
                new_frames if current_frames is None else new_frames + current_frames
            )
            final_hit_rate = (
                final_om_hits / final_om_frames * 100.0 if final_om_frames != 0 else 0.0
            )
            final_runtime_status = DBIndexingResultRunning(
                _DUMMY_STREAM,
                _DUMMY_JOB_ID,
                DBIndexingFOM(
                    hit_rate=final_hit_rate,
                    indexing_rate=latest_runtime_status.fom.indexing_rate,
                    indexed_frames=latest_runtime_status.fom.indexed_frames,
                ),
            )
            if latest_indexing_result is None:
                await self._db.create_indexing_result(
                    conn,
                    DBIndexingResultInput(
                        created=datetime.datetime.utcnow(),
                        run_id=latest_run.id,
                        frames=final_om_frames,
                        hits=final_om_hits,
                        not_indexed_frames=0,
                        runtime_status=DBIndexingResultRunning(
                            _DUMMY_STREAM, _DUMMY_JOB_ID, final_runtime_status.fom
                        ),
                    ),
                )
            else:
                await self._db.update_indexing_result(
                    conn,
                    latest_indexing_result.id,
                    DBIndexingResultInput(
                        created=datetime.datetime.utcnow(),
                        run_id=latest_run.id,
                        frames=final_om_frames,
                        hits=final_om_hits,
                        not_indexed_frames=not_indexed_frames,
                        runtime_status=final_runtime_status,
                    ),
                )
