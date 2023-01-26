from datetime import datetime

from amarcord.amici.crystfel.util import ATTRIBUTO_PROTEIN
from amarcord.amici.om.client import ATTRIBUTO_STOPPED
from amarcord.amici.om.client import OmZMQProcessor
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoTypeChemical
from amarcord.db.attributo_type import AttributoTypeString
from amarcord.db.chemical_type import ChemicalType
from amarcord.db.tables import create_tables_from_metadata


async def _get_db() -> AsyncDB:
    context = AsyncDBContext("sqlite+aiosqlite://")
    db = AsyncDB(context, create_tables_from_metadata(context.metadata))
    await db.migrate()
    return db


async def test_process_data_without_runs() -> None:
    db = await _get_db()

    processor = OmZMQProcessor(db)

    # Send first frame, will be ignored
    await processor.process_data(
        {
            "num_hits": 0,
            "num_events": 0,
            "start_timestamp": 100,
        }
    )
    # Second frame, but we have no runs. Shouldn't crash at least
    await processor.process_data(
        {
            "num_hits": 10,
            "num_events": 100,
            "start_timestamp": 100,
        }
    )


async def test_process_data_latest_run() -> None:
    db = await _get_db()

    processor = OmZMQProcessor(db)

    async with db.begin() as conn:
        await db.create_attributo(
            conn,
            name=ATTRIBUTO_PROTEIN,
            description="",
            group="",
            associated_table=AssociatedTable.RUN,
            type_=AttributoTypeChemical(),
        )
        await db.create_attributo(
            conn,
            name="point group",
            description="",
            group="",
            associated_table=AssociatedTable.CHEMICAL,
            type_=AttributoTypeString(),
        )
        await db.create_attributo(
            conn,
            name="cell description",
            description="",
            group="",
            associated_table=AssociatedTable.CHEMICAL,
            type_=AttributoTypeString(),
        )
        attributi = await db.retrieve_attributi(conn, associated_table=None)
        chemical_id = await db.create_chemical(
            conn,
            name="nsp3",
            type_=ChemicalType.CRYSTAL,
            attributi=AttributiMap.from_types_and_raw(
                types=attributi,
                chemical_ids=[],
                raw_attributi={
                    AttributoId("point group"): "4/mmm",
                    AttributoId(
                        "cell description"
                    ): "tetragonal P c (79.2 79.2 38.0) (90 90 90)",
                },
            ),
        )
        await db.create_run(
            conn,
            1,
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_json(
                types=attributi,
                chemical_ids=[chemical_id],
                raw_attributi={ATTRIBUTO_PROTEIN: chemical_id},
            ),
            keep_manual_attributes_from_previous_run=False,
        )

    # Send first frame, will be ignored
    await processor.process_data(
        {
            "num_hits": 3,
            "num_events": 5,
            "start_timestamp": 100,
        }
    )
    # Second frame
    await processor.process_data(
        {
            "num_hits": 7,
            "num_events": 11,
            "start_timestamp": 100,
        }
    )

    async with db.read_only_connection() as conn:
        run = await db.retrieve_latest_run(conn, attributi)
        assert run is not None

        indexing_result = await db.retrieve_indexing_result_for_run(conn, run.id)

        assert indexing_result is not None
        assert indexing_result.hits == 4
        assert indexing_result.frames == 6

    # Now stop the run and add some frames
    async with db.begin() as conn:
        run = await db.retrieve_latest_run(conn, attributi)
        run.attributi.append_single(ATTRIBUTO_STOPPED, datetime.utcnow())  # type: ignore
        await db.update_run_attributi(conn, run.id, run.attributi)  # type: ignore

    await processor.process_data(
        {
            "num_hits": 9,
            "num_events": 13,
            "start_timestamp": 100,
        }
    )

    async with db.read_only_connection() as conn:
        run = await db.retrieve_latest_run(conn, attributi)
        assert run is not None

        indexing_result = await db.retrieve_indexing_result_for_run(conn, run.id)

        assert indexing_result is not None

        # 4 because the first frame gets ignored as a baseline
        assert indexing_result.hits == 4
        assert indexing_result.frames == 6
