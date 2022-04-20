from datetime import datetime

from amarcord.amici.onda.client import (
    OnDAZMQProcessor,
    ATTRIBUTO_STOPPED,
    ATTRIBUTO_NUMBER_OF_FRAMES,
    ATTRIBUTO_NUMBER_OF_HITS,
)
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.tables import create_tables_from_metadata


async def _get_db() -> AsyncDB:
    context = AsyncDBContext("sqlite+aiosqlite://")
    db = AsyncDB(context, create_tables_from_metadata(context.metadata))
    await db.migrate()
    return db


async def test_process_data_latest_run():
    db = await _get_db()

    processor = OnDAZMQProcessor(db)

    await processor.init()

    async with db.begin() as conn:
        attributi = await db.retrieve_attributi(
            conn, associated_table=AssociatedTable.RUN
        )
        await db.create_run(
            conn,
            1,
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_json(
                types=attributi, sample_ids=[], raw_attributi={}
            ),
            keep_manual_attributes_from_previous_run=False,
        )

    # Send first frame
    await processor.process_data([{"frame_is_hit": True}, {"frame_is_hit": False}])

    async with db.read_only_connection() as conn:
        run = await db.retrieve_latest_run(conn, attributi)

        assert run.attributi.select_int_unsafe(ATTRIBUTO_NUMBER_OF_HITS) == 1
        assert run.attributi.select_int_unsafe(ATTRIBUTO_NUMBER_OF_FRAMES) == 2

    # Now stop the run and add some frames
    async with db.begin() as conn:
        run = await db.retrieve_latest_run(conn, attributi)
        run.attributi.append_single(ATTRIBUTO_STOPPED, datetime.utcnow())
        await db.update_run_attributi(conn, run.id, run.attributi)

    await processor.process_data([{"frame_is_hit": True}, {"frame_is_hit": False}])

    async with db.read_only_connection() as conn:
        run = await db.retrieve_latest_run(conn, attributi)

        assert run.attributi.select_int_unsafe(ATTRIBUTO_NUMBER_OF_HITS) == 1
        assert run.attributi.select_int_unsafe(ATTRIBUTO_NUMBER_OF_FRAMES) == 2
