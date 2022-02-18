from datetime import datetime

from amarcord.amici.om.zeromq import (
    OmZMQProcessor,
    ATTRIBUTO_NUMBER_OF_HITS,
    ATTRIBUTO_NUMBER_OF_FRAMES,
    ATTRIBUTO_STOPPED,
)
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.attributi import datetime_to_attributo_int
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.attributo_type import AttributoTypeDateTime
from amarcord.db.dbcontext import CreationMode
from amarcord.db.tables import create_tables_from_metadata


async def _get_db() -> AsyncDB:
    context = AsyncDBContext("sqlite+aiosqlite://")
    db = AsyncDB(context, create_tables_from_metadata(context.metadata))
    await context.create_all(CreationMode.DONT_CHECK)
    return db


async def test_process_data_without_runs():
    db = await _get_db()

    processor = OmZMQProcessor(db)

    await processor.init()

    # Send first frame, will be ignored
    await processor.process_data(
        {
            "total_hits": 0,
            "total_frames": 0,
            "timestamp": 100,
        }
    )
    # Second frame, but we have no runs. Shouldn't crash at least
    await processor.process_data(
        {
            "total_hits": 10,
            "total_frames": 100,
            "timestamp": 100,
        }
    )


async def test_process_data_latest_run():
    db = await _get_db()

    processor = OmZMQProcessor(db)

    await processor.init()

    async with db.begin() as conn:
        await db.create_attributo(
            conn,
            ATTRIBUTO_STOPPED,
            "",
            "kamzik",
            AssociatedTable.RUN,
            AttributoTypeDateTime(),
        )

        attributi = await db.retrieve_attributi(
            conn, associated_table=AssociatedTable.RUN
        )
        await db.create_run(
            conn,
            1,
            AttributiMap.from_types_and_json(
                types=attributi, sample_ids=[], raw_attributi={}
            ),
        )

    # Send first frame, will be ignored
    await processor.process_data(
        {
            "total_hits": 3,
            "total_frames": 5,
            "timestamp": 100,
        }
    )
    # Second frame
    await processor.process_data(
        {
            "total_hits": 7,
            "total_frames": 11,
            "timestamp": 100,
        }
    )

    async with db.connect() as conn:
        run = await db.retrieve_latest_run(conn, attributi)

        # 4 because the first frame gets ignored as a baseline
        assert run.attributi.select_int_unsafe(ATTRIBUTO_NUMBER_OF_HITS) == 4
        assert run.attributi.select_int_unsafe(ATTRIBUTO_NUMBER_OF_FRAMES) == 6

    # Now stop the run and add some frames
    async with db.begin() as conn:
        run = await db.retrieve_latest_run(conn, attributi)
        run.attributi.append_single(
            ATTRIBUTO_STOPPED, datetime_to_attributo_int(datetime.utcnow())
        )
        await db.update_run_attributi(conn, run.id, run.attributi)

    await processor.process_data(
        {
            "total_hits": 9,
            "total_frames": 13,
            "timestamp": 100,
        }
    )

    async with db.connect() as conn:
        run = await db.retrieve_latest_run(conn, attributi)

        # 4 because the first frame gets ignored as a baseline
        assert run.attributi.select_int_unsafe(ATTRIBUTO_NUMBER_OF_HITS) == 4
        assert run.attributi.select_int_unsafe(ATTRIBUTO_NUMBER_OF_FRAMES) == 6
