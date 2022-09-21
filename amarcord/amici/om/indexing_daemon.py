import asyncio
import datetime
import os
from dataclasses import dataclass
from dataclasses import replace
from pathlib import Path
from time import time
from typing import Generator
from typing import TextIO
from typing import TypeAlias

import structlog
from structlog.stdlib import BoundLogger

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.async_dbcontext import Connection
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.attributi import ATTRIBUTO_STARTED
from amarcord.db.attributi import ATTRIBUTO_STOPPED
from amarcord.db.attributi import datetime_from_float_in_seconds
from amarcord.db.indexing_result import DBIndexingFOM
from amarcord.db.indexing_result import DBIndexingResultInput
from amarcord.db.indexing_result import DBIndexingResultRunning
from amarcord.db.table_classes import DBRun

_SECONDS_TO_BATCH = 10.0

logger = structlog.stdlib.get_logger(__name__)


@dataclass(frozen=True, eq=True)
class _IndexingDataForRun:
    indexed_crystals: int
    indexed_frames: int
    not_indexed_frames: int


async def _update_run(
    parent_logger: BoundLogger,
    db: AsyncDB,
    conn: Connection,
    run_id: int,
    indexing_data: _IndexingDataForRun,
) -> None:
    run_logger = parent_logger.bind(run_id=run_id)

    current_ir = await db.retrieve_indexing_result_for_run(conn, run_id)
    if current_ir is None:
        run_logger.info("run doesn't have indexing result, creating one")
        await db.create_indexing_result(
            conn,
            DBIndexingResultInput(
                created=datetime.datetime.utcnow(),
                run_id=run_id,
                frames=0,
                hits=0,
                not_indexed_frames=indexing_data.not_indexed_frames,
                runtime_status=DBIndexingResultRunning(
                    stream_file=Path("dummy.stream"),
                    job_id=0,
                    fom=DBIndexingFOM(
                        hit_rate=0.0,
                        indexing_rate=indexing_data.indexed_frames
                        / (
                            indexing_data.indexed_frames
                            + indexing_data.not_indexed_frames
                        )
                        * 100.0,
                        indexed_frames=indexing_data.indexed_frames,
                    ),
                ),
            ),
        )
    else:
        if current_ir.runtime_status is None:
            run_logger.error(
                "Queued indexing status? Is Om mixed with CrystFEL online?"
            )
            return
        total_indexed_frames = (
            indexing_data.indexed_frames + current_ir.runtime_status.fom.indexed_frames
        )
        total_not_indexed_frames = indexing_data.not_indexed_frames + (
            current_ir.not_indexed_frames
            if current_ir.not_indexed_frames is not None
            else 0
        )
        run_logger.info(
            f"updating indexing result, new indexed/not indexed frames: {total_indexed_frames}/{total_not_indexed_frames}"
        )
        await db.update_indexing_result(
            conn,
            current_ir.id,
            DBIndexingResultInput(
                created=current_ir.created,
                run_id=current_ir.run_id,
                frames=current_ir.frames,
                hits=current_ir.hits,
                not_indexed_frames=total_not_indexed_frames,
                runtime_status=DBIndexingResultRunning(
                    stream_file=Path("dummy.stream"),
                    job_id=0,
                    fom=replace(
                        current_ir.runtime_status.fom,
                        indexing_rate=total_indexed_frames
                        / (total_not_indexed_frames + total_indexed_frames)
                        * 100.0,
                        indexed_frames=total_indexed_frames,
                    ),
                ),
            ),
        )


Chunk: TypeAlias = tuple[int, datetime.datetime]


def parse_stream(stream: TextIO) -> Generator[Chunk, None, None]:
    crystals_in_chunk = 0
    chunk_timestamp: None | datetime.datetime = None
    line_no = 1
    timestamp_prefix = "header/float/timestamp = "
    for line in stream:
        if line.startswith("----- Begin chunk"):
            crystals_in_chunk = 0
        elif line.startswith("----- End chunk"):
            if chunk_timestamp is None:
                logger.info("chunk without timestamp received")
            else:
                yield crystals_in_chunk, chunk_timestamp
        elif line.startswith("--- Begin crystal"):
            crystals_in_chunk += 1
        elif line.startswith(timestamp_prefix):
            chunk_timestamp = datetime_from_float_in_seconds(
                float(line[len(timestamp_prefix) :])
            )
        line_no += 1


def update_runs_from_chunks(
    parent_logger: BoundLogger, chunks: list[Chunk], runs: list[DBRun]
) -> dict[int, _IndexingDataForRun]:
    if not chunks:
        return {}
    new_run_data: dict[int, _IndexingDataForRun] = {}
    for crystals_in_chunk, chunk_timestamp in chunks:
        chunk_log = parent_logger.bind(chunk_timestamp=chunk_timestamp)
        for run in runs:
            run_logger = chunk_log.bind(run_id=run.id)
            attributi = run.attributi
            started = attributi.select_datetime_unsafe(ATTRIBUTO_STARTED)
            stopped = attributi.select_datetime(ATTRIBUTO_STOPPED)
            run_id = run.id

            run_logger.info(f"started: {started}, stopped: {stopped}")
            if (
                chunk_timestamp < started
                or stopped is not None
                and chunk_timestamp > stopped
            ):
                run_logger.info(f"chunk time {chunk_timestamp}, dropping")
                continue

            current_run_data = new_run_data.get(
                run_id,
                _IndexingDataForRun(0, 0, 0),
            )
            indexed_frames = current_run_data.indexed_frames + (
                1 if crystals_in_chunk else 0
            )
            not_indexed_frames = current_run_data.not_indexed_frames + (
                0 if crystals_in_chunk else 1
            )
            new_run_data[run_id] = _IndexingDataForRun(
                indexed_crystals=current_run_data.indexed_crystals + crystals_in_chunk,
                indexed_frames=indexed_frames,
                not_indexed_frames=not_indexed_frames,
            )
            # We've found one run. That's enough.
            break
    parent_logger.info(f"new run data: {new_run_data}")
    return new_run_data


async def _update_runs_in_db(
    parent_logger: BoundLogger, db: AsyncDB, chunks: list[Chunk]
) -> None:
    async with db.read_only_connection() as conn:
        attributi = await db.retrieve_attributi(
            conn, associated_table=AssociatedTable.RUN
        )
        runs = await db.retrieve_runs(conn, attributi)
    new_run_data = update_runs_from_chunks(
        parent_logger,
        chunks,
        runs,
    )
    async with db.begin() as conn:
        for run_id, run_attributi in new_run_data.items():
            await _update_run(parent_logger, db, conn, run_id, run_attributi)


async def om_indexing_loop(db: AsyncDB, watch_file: Path) -> None:
    last_request = time()
    chunks: list[Chunk] = []
    last_position: None | int = None
    while True:
        iteration_logger = logger.bind(last_position=last_position)
        iteration_logger.info("stream file loop restarting")

        with watch_file.open("r") as f:
            if last_position is None:
                f.seek(0, os.SEEK_END)
            else:
                f.seek(last_position, os.SEEK_SET)
            for this_crystals_in_chunk, this_chunk_timestamp in parse_stream(f):
                iteration_logger.info(
                    f"new chunk with {this_crystals_in_chunk} crystal(s), time {this_chunk_timestamp}"
                )
                # Chunk timestamp is in seconds, we store milliseconds
                chunks.append((this_crystals_in_chunk, this_chunk_timestamp))
                if time() - last_request < _SECONDS_TO_BATCH:
                    continue
                await _update_runs_in_db(iteration_logger, db, chunks)
                chunks.clear()
            await _update_runs_in_db(iteration_logger, db, chunks)
            last_position = f.tell()

        await asyncio.sleep(5.0)
