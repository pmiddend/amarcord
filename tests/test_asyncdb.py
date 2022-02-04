import pytest

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.attributi import AttributoConversionFlags
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.attributo_type import (
    AttributoTypeInt,
    AttributoTypeString,
    AttributoTypeSample,
)
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.dbcontext import CreationMode
from amarcord.db.tables import create_tables_from_metadata

TEST_ATTRIBUTO_VALUE = 3

TEST_SAMPLE_NAME = "samplename"

TEST_RUN_ID = 1

TEST_ATTRIBUTO_GROUP = "testgroup"

TEST_ATTRIBUTO_DESCRIPTION = "testdescription"

TEST_ATTRIBUTO_NAME = "testname"


async def _get_db() -> AsyncDB:
    context = AsyncDBContext("sqlite+aiosqlite://")
    db = AsyncDB(context, create_tables_from_metadata(context.metadata))
    await context.create_all(CreationMode.DONT_CHECK)
    return db


async def test_create_and_retrieve_attributo() -> None:
    """Just a really simple test: create a single attributo for a sample, then retrieve it"""
    db = await _get_db()

    async with db.begin() as conn:
        await db.create_attributo(
            conn,
            TEST_ATTRIBUTO_NAME,
            TEST_ATTRIBUTO_DESCRIPTION,
            TEST_ATTRIBUTO_GROUP,
            AssociatedTable.SAMPLE,
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(conn, AssociatedTable.SAMPLE)
        assert len(attributi) == 1
        assert attributi[0].attributo_type == AttributoTypeInt()
        assert attributi[0].name == TEST_ATTRIBUTO_NAME
        assert attributi[0].description == TEST_ATTRIBUTO_DESCRIPTION
        assert attributi[0].group == TEST_ATTRIBUTO_GROUP

        # Check if the filter for the table works: there should be no attributi for runs
        assert not await db.retrieve_attributi(conn, AssociatedTable.RUN)


async def test_create_and_delete_unused_attributo() -> None:
    """Create an attributo, then delete it again"""

    db = await _get_db()

    async with db.begin() as conn:
        await db.create_attributo(
            conn,
            TEST_ATTRIBUTO_NAME,
            TEST_ATTRIBUTO_DESCRIPTION,
            TEST_ATTRIBUTO_GROUP,
            AssociatedTable.SAMPLE,
            AttributoTypeInt(),
        )

        await db.delete_attributo(conn, TEST_ATTRIBUTO_NAME)

        assert not await db.retrieve_attributi(conn, associated_table=None)


async def test_create_and_retrieve_sample() -> None:
    """Create an attributo, then a sample, and then retrieve that"""

    db = await _get_db()

    async with db.begin() as conn:
        await db.create_attributo(
            conn,
            TEST_ATTRIBUTO_NAME,
            TEST_ATTRIBUTO_DESCRIPTION,
            TEST_ATTRIBUTO_GROUP,
            AssociatedTable.SAMPLE,
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        sample_id = await db.create_sample(
            conn,
            name=TEST_SAMPLE_NAME,
            attributi=AttributiMap.from_types_and_json(
                attributi,
                sample_ids=[],
                raw_attributi={TEST_ATTRIBUTO_NAME: TEST_ATTRIBUTO_VALUE},
            ),
        )

        assert isinstance(sample_id, int)

        samples = await db.retrieve_samples(conn, attributi)

        assert len(samples) == 1
        assert samples[0].name == TEST_SAMPLE_NAME
        assert samples[0].id == sample_id
        assert not samples[0].files
        assert (
            samples[0].attributi.select_int_unsafe(TEST_ATTRIBUTO_NAME)
            == TEST_ATTRIBUTO_VALUE
        )


async def test_create_and_update_sample() -> None:
    """Create an attributo, then a sample, and then update that"""

    db = await _get_db()

    async with db.begin() as conn:
        await db.create_attributo(
            conn,
            TEST_ATTRIBUTO_NAME,
            TEST_ATTRIBUTO_DESCRIPTION,
            TEST_ATTRIBUTO_GROUP,
            AssociatedTable.SAMPLE,
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        sample_id = await db.create_sample(
            conn,
            name=TEST_SAMPLE_NAME,
            attributi=AttributiMap.from_types_and_json(
                attributi,
                sample_ids=[],
                raw_attributi={TEST_ATTRIBUTO_NAME: TEST_ATTRIBUTO_VALUE},
            ),
        )

        await db.update_sample(
            conn,
            sample_id,
            name=TEST_SAMPLE_NAME + "1",
            attributi=AttributiMap.from_types_and_json(
                attributi,
                sample_ids=[],
                raw_attributi={TEST_ATTRIBUTO_NAME: TEST_ATTRIBUTO_VALUE + 1},
            ),
        )

        samples = await db.retrieve_samples(conn, attributi)

        assert len(samples) == 1
        assert samples[0].name == TEST_SAMPLE_NAME + "1"
        assert samples[0].id == sample_id
        assert (
            samples[0].attributi.select_int_unsafe(TEST_ATTRIBUTO_NAME)
            == TEST_ATTRIBUTO_VALUE + 1
        )


async def test_create_and_delete_sample() -> None:
    """Create an attributo, then a sample, delete the sample again"""

    db = await _get_db()

    async with db.begin() as conn:
        await db.create_attributo(
            conn,
            TEST_ATTRIBUTO_NAME,
            TEST_ATTRIBUTO_DESCRIPTION,
            TEST_ATTRIBUTO_GROUP,
            AssociatedTable.SAMPLE,
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        sample_id = await db.create_sample(
            conn,
            name=TEST_SAMPLE_NAME,
            attributi=AttributiMap.from_types_and_json(
                attributi,
                sample_ids=[],
                raw_attributi={TEST_ATTRIBUTO_NAME: TEST_ATTRIBUTO_VALUE},
            ),
        )

        await db.delete_sample(conn, sample_id, delete_in_runs=True)
        assert not await db.retrieve_samples(conn, attributi)


async def test_create_attributo_and_sample_then_change_attributo() -> None:
    """Create an attributo, then a sample, then change the attributo. Should propagate to the sample"""

    db = await _get_db()

    async with db.begin() as conn:
        await db.create_attributo(
            conn,
            TEST_ATTRIBUTO_NAME,
            TEST_ATTRIBUTO_DESCRIPTION,
            TEST_ATTRIBUTO_GROUP,
            AssociatedTable.SAMPLE,
            # We create the attributo as int, then convert to string
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        await db.create_sample(
            conn,
            name=TEST_SAMPLE_NAME,
            attributi=AttributiMap.from_types_and_json(
                attributi,
                sample_ids=[],
                raw_attributi={TEST_ATTRIBUTO_NAME: TEST_ATTRIBUTO_VALUE},
            ),
        )

        await db.update_attributo(
            conn,
            name=TEST_ATTRIBUTO_NAME,
            conversion_flags=AttributoConversionFlags(ignore_units=False),
            new_attributo=DBAttributo(
                # We try to change all the attributes
                name=TEST_ATTRIBUTO_NAME + "1",
                description=TEST_ATTRIBUTO_DESCRIPTION + "1",
                group=TEST_ATTRIBUTO_GROUP + "1",
                associated_table=AssociatedTable.SAMPLE,
                attributo_type=AttributoTypeString(),
            ),
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        assert len(attributi) == 1
        assert attributi[0].name == TEST_ATTRIBUTO_NAME + "1"
        assert attributi[0].description == TEST_ATTRIBUTO_DESCRIPTION + "1"
        assert attributi[0].group == TEST_ATTRIBUTO_GROUP + "1"

        samples = await db.retrieve_samples(conn, attributi)
        assert samples[0].attributi.select_int(TEST_ATTRIBUTO_NAME) is None
        assert samples[0].attributi.select_string(TEST_ATTRIBUTO_NAME + "1") is not None


async def test_create_attributo_and_sample_then_delete_attributo() -> None:
    """Create an attributo, then a sample, then delete the attributo. Should propagate to the sample"""

    db = await _get_db()

    async with db.begin() as conn:
        await db.create_attributo(
            conn,
            TEST_ATTRIBUTO_NAME,
            TEST_ATTRIBUTO_DESCRIPTION,
            TEST_ATTRIBUTO_GROUP,
            AssociatedTable.SAMPLE,
            # We create the attributo as int, then convert to string
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        await db.create_sample(
            conn,
            name=TEST_SAMPLE_NAME,
            attributi=AttributiMap.from_types_and_json(
                attributi,
                sample_ids=[],
                raw_attributi={TEST_ATTRIBUTO_NAME: TEST_ATTRIBUTO_VALUE},
            ),
        )

        await db.delete_attributo(conn, TEST_ATTRIBUTO_NAME)

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        samples = await db.retrieve_samples(conn, attributi)
        assert samples[0].attributi.select_int(TEST_ATTRIBUTO_NAME) is None


async def test_create_and_retrieve_run() -> None:
    """Create an attributo, then a run, and then retrieve that"""

    db = await _get_db()

    async with db.begin() as conn:
        await db.create_attributo(
            conn,
            TEST_ATTRIBUTO_NAME,
            TEST_ATTRIBUTO_DESCRIPTION,
            TEST_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        await db.create_run(
            conn,
            run_id=TEST_RUN_ID,
            attributi=AttributiMap.from_types_and_json(
                attributi,
                sample_ids=await db.retrieve_sample_ids(conn),
                raw_attributi={TEST_ATTRIBUTO_NAME: TEST_ATTRIBUTO_VALUE},
            ),
        )

        runs = await db.retrieve_runs(conn, attributi)

        assert len(runs) == 1
        assert runs[0].id == TEST_RUN_ID
        assert not runs[0].files
        assert (
            runs[0].attributi.select_int_unsafe(TEST_ATTRIBUTO_NAME)
            == TEST_ATTRIBUTO_VALUE
        )


async def test_create_attributo_and_run_then_change_attributo() -> None:
    """Create an attributo, then a run, then change the attributo. Should propagate to the run"""

    db = await _get_db()

    async with db.begin() as conn:
        await db.create_attributo(
            conn,
            TEST_ATTRIBUTO_NAME,
            TEST_ATTRIBUTO_DESCRIPTION,
            TEST_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            # We create the attributo as int, then convert to string
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        await db.create_run(
            conn,
            run_id=TEST_RUN_ID,
            attributi=AttributiMap.from_types_and_json(
                attributi,
                sample_ids=await db.retrieve_sample_ids(conn),
                raw_attributi={TEST_ATTRIBUTO_NAME: TEST_ATTRIBUTO_VALUE},
            ),
        )

        await db.update_attributo(
            conn,
            name=TEST_ATTRIBUTO_NAME,
            conversion_flags=AttributoConversionFlags(ignore_units=False),
            new_attributo=DBAttributo(
                # We try to change all the attributes
                name=TEST_ATTRIBUTO_NAME + "1",
                description=TEST_ATTRIBUTO_DESCRIPTION + "1",
                group=TEST_ATTRIBUTO_GROUP + "1",
                associated_table=AssociatedTable.RUN,
                attributo_type=AttributoTypeString(),
            ),
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        runs = await db.retrieve_runs(conn, attributi)
        assert runs[0].attributi.select_int(TEST_ATTRIBUTO_NAME) is None
        assert runs[0].attributi.select_string(TEST_ATTRIBUTO_NAME + "1") is not None


async def test_create_attributo_and_run_then_delete_attributo() -> None:
    """Create an attributo, then a run, then change the attributo. Should propagate to the run"""

    db = await _get_db()

    async with db.begin() as conn:
        await db.create_attributo(
            conn,
            TEST_ATTRIBUTO_NAME,
            TEST_ATTRIBUTO_DESCRIPTION,
            TEST_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            # We create the attributo as int, then convert to string
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        await db.create_run(
            conn,
            run_id=TEST_RUN_ID,
            attributi=AttributiMap.from_types_and_json(
                attributi,
                sample_ids=await db.retrieve_sample_ids(conn),
                raw_attributi={TEST_ATTRIBUTO_NAME: TEST_ATTRIBUTO_VALUE},
            ),
        )

        await db.delete_attributo(conn, TEST_ATTRIBUTO_NAME)

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        runs = await db.retrieve_runs(conn, attributi)
        assert runs[0].attributi.select_int(TEST_ATTRIBUTO_NAME) is None


async def test_create_attributo_and_run_and_sample_for_run_then_delete_sample() -> None:
    """This is a little bit of an edge case: we have an attributo that signifies the sample of a run, and we create a run and a sample, and then we delete the sample"""

    db = await _get_db()

    async with db.begin() as conn:
        await db.create_attributo(
            conn,
            TEST_ATTRIBUTO_NAME,
            TEST_ATTRIBUTO_DESCRIPTION,
            TEST_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            AttributoTypeSample(),
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        sample_id = await db.create_sample(
            conn,
            name=TEST_SAMPLE_NAME,
            attributi=AttributiMap.from_types_and_json(
                attributi, sample_ids=[], raw_attributi={}
            ),
        )

        await db.create_run(
            conn,
            run_id=TEST_RUN_ID,
            attributi=AttributiMap.from_types_and_json(
                attributi,
                sample_ids=await db.retrieve_sample_ids(conn),
                raw_attributi={TEST_ATTRIBUTO_NAME: sample_id},
            ),
        )

        with pytest.raises(Exception):
            # This doesn't work, because the sample is being used
            await db.delete_sample(conn, sample_id, delete_in_runs=False)

        # This works, because we explicitly say we want to delete it from the runs
        await db.delete_sample(conn, sample_id, delete_in_runs=True)

        runs = await db.retrieve_runs(conn, attributi)
        assert runs[0].attributi.select_sample_id(TEST_ATTRIBUTO_NAME) is None
