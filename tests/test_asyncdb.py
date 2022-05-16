import datetime
from pathlib import Path
from typing import Dict

import pytest

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import AsyncDB, ATTRIBUTO_GROUP_MANUAL, create_workbook
from amarcord.db.attributi import (
    AttributoConversionFlags,
    ATTRIBUTO_STARTED,
    ATTRIBUTO_STOPPED,
)
from amarcord.db.attributi_map import AttributiMap, JsonAttributiMap
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import (
    AttributoTypeInt,
    AttributoTypeString,
    AttributoTypeSample,
)
from amarcord.db.cfel_analysis_result import DBCFELAnalysisResult
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.tables import create_tables_from_metadata

EVENT_SOURCE = "P11User"

TEST_ATTRIBUTO_VALUE = 3
TEST_SECOND_ATTRIBUTO_VALUE = 4

TEST_SAMPLE_NAME = "samplename"

TEST_RUN_ID = 1

TEST_ATTRIBUTO_GROUP = "testgroup"

TEST_ATTRIBUTO_DESCRIPTION = "testdescription"

TEST_ATTRIBUTO_NAME = AttributoId("testname")
TEST_SECOND_ATTRIBUTO_NAME = AttributoId("testname1")


async def _get_db() -> AsyncDB:
    context = AsyncDBContext("sqlite+aiosqlite://")
    db = AsyncDB(context, create_tables_from_metadata(context.metadata))
    await db.migrate()
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
        assert not [
            a
            for a in await db.retrieve_attributi(conn, AssociatedTable.RUN)
            if a.name == TEST_ATTRIBUTO_NAME
        ]


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

        assert not [
            a
            for a in await db.retrieve_attributi(conn, associated_table=None)
            if a.name == TEST_ATTRIBUTO_NAME
        ]


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

        sample = await db.retrieve_sample(conn, sample_id, attributi)
        assert sample is not None
        assert sample.id == sample_id
        assert sample.name == TEST_SAMPLE_NAME


async def test_create_and_update_sample() -> None:
    """Create two attributi, then a sample, and then update that"""

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
        await db.create_attributo(
            conn,
            TEST_SECOND_ATTRIBUTO_NAME,
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
                raw_attributi={
                    TEST_ATTRIBUTO_NAME: TEST_ATTRIBUTO_VALUE,
                    TEST_SECOND_ATTRIBUTO_NAME: TEST_SECOND_ATTRIBUTO_VALUE,
                },
            ),
        )

        await db.update_sample(
            conn,
            sample_id,
            name=TEST_SAMPLE_NAME + "1",
            attributi=AttributiMap.from_types_and_json(
                attributi,
                sample_ids=[],
                raw_attributi={
                    TEST_ATTRIBUTO_NAME: TEST_ATTRIBUTO_VALUE + 1,
                    TEST_SECOND_ATTRIBUTO_NAME: TEST_SECOND_ATTRIBUTO_VALUE,
                },
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
        assert (
            samples[0].attributi.select_int_unsafe(TEST_SECOND_ATTRIBUTO_NAME)
            == TEST_SECOND_ATTRIBUTO_VALUE
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

        await db.delete_sample(conn, sample_id, delete_in_dependencies=True)
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
                name=AttributoId(str(TEST_ATTRIBUTO_NAME) + "1"),
                description=TEST_ATTRIBUTO_DESCRIPTION + "1",
                group=TEST_ATTRIBUTO_GROUP + "1",
                associated_table=AssociatedTable.SAMPLE,
                attributo_type=AttributoTypeString(),
            ),
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)
        test_attributi = [a for a in attributi if a.name == TEST_ATTRIBUTO_NAME + "1"]

        assert len(test_attributi) == 1
        assert test_attributi[0].name == TEST_ATTRIBUTO_NAME + "1"
        assert test_attributi[0].description == TEST_ATTRIBUTO_DESCRIPTION + "1"
        assert test_attributi[0].group == TEST_ATTRIBUTO_GROUP + "1"

        samples = await db.retrieve_samples(conn, attributi)
        assert samples[0].attributi.select_int(TEST_ATTRIBUTO_NAME) is None
        assert (
            samples[0].attributi.select_string(
                AttributoId(str(TEST_ATTRIBUTO_NAME) + "1")
            )
            is not None
        )


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


async def test_create_and_retrieve_runs() -> None:
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
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_json(
                attributi,
                sample_ids=await db.retrieve_sample_ids(conn),
                raw_attributi={TEST_ATTRIBUTO_NAME: TEST_ATTRIBUTO_VALUE},
            ),
            keep_manual_attributes_from_previous_run=False,
        )

        runs = await db.retrieve_runs(conn, attributi)

        assert len(runs) == 1
        assert runs[0].id == TEST_RUN_ID
        assert not runs[0].files
        assert (
            runs[0].attributi.select_int_unsafe(TEST_ATTRIBUTO_NAME)
            == TEST_ATTRIBUTO_VALUE
        )


async def test_create_and_retrieve_run() -> None:
    """Create an attributo, then a run, and then retrieve just that run"""

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
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_json(
                attributi,
                sample_ids=await db.retrieve_sample_ids(conn),
                raw_attributi={TEST_ATTRIBUTO_NAME: TEST_ATTRIBUTO_VALUE},
            ),
            keep_manual_attributes_from_previous_run=False,
        )

        run = await db.retrieve_run(conn, TEST_RUN_ID, attributi)

        assert run is not None
        assert run.id == TEST_RUN_ID
        assert not run.files
        assert (
            run.attributi.select_int_unsafe(TEST_ATTRIBUTO_NAME) == TEST_ATTRIBUTO_VALUE
        )

        run = await db.retrieve_latest_run(conn, attributi)

        assert run is not None
        assert run.id == TEST_RUN_ID
        assert not run.files
        assert (
            run.attributi.select_int_unsafe(TEST_ATTRIBUTO_NAME) == TEST_ATTRIBUTO_VALUE
        )


async def test_create_run_and_then_next_run_using_previous_attributi() -> None:
    """Create a run with some attributi, then another run and test the "keep attributi from previous" feature"""

    db = await _get_db()

    async with db.begin() as conn:
        # The mechanism to copy over attributes from the previous run is hard-coded to "manual" attributi, so let's
        # create one that's manual and one that isn't
        await db.create_attributo(
            conn,
            TEST_ATTRIBUTO_NAME,
            TEST_ATTRIBUTO_DESCRIPTION,
            ATTRIBUTO_GROUP_MANUAL,
            AssociatedTable.RUN,
            AttributoTypeInt(),
        )
        second_test_attribute = AttributoId(str(TEST_ATTRIBUTO_NAME) + "2")
        await db.create_attributo(
            conn,
            second_test_attribute,
            TEST_ATTRIBUTO_DESCRIPTION,
            TEST_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        await db.create_run(
            conn,
            run_id=TEST_RUN_ID,
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_json(
                attributi,
                sample_ids=await db.retrieve_sample_ids(conn),
                # Only one of the two attributi here (the manual one)
                raw_attributi={TEST_ATTRIBUTO_NAME: TEST_ATTRIBUTO_VALUE},
            ),
            # Flag doesn't matter if it's just one run
            keep_manual_attributes_from_previous_run=False,
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        second_test_attribute_value = TEST_ATTRIBUTO_VALUE + 1
        await db.create_run(
            conn,
            # Next Run ID
            run_id=TEST_RUN_ID + 1,
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_json(
                attributi,
                sample_ids=await db.retrieve_sample_ids(conn),
                # The other attributo with a different value
                raw_attributi={second_test_attribute: second_test_attribute_value},
            ),
            # Keep previous (manual) attributi
            keep_manual_attributes_from_previous_run=True,
        )

        runs = await db.retrieve_runs(conn, attributi)
        assert len(runs) == 2
        # Assume ordering by ID descending
        assert runs[0].id == TEST_RUN_ID + 1
        assert runs[0].attributi.select_int(TEST_ATTRIBUTO_NAME) == TEST_ATTRIBUTO_VALUE
        assert (
            runs[0].attributi.select_int(second_test_attribute)
            == second_test_attribute_value
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
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_json(
                attributi,
                sample_ids=await db.retrieve_sample_ids(conn),
                raw_attributi={TEST_ATTRIBUTO_NAME: TEST_ATTRIBUTO_VALUE},
            ),
            keep_manual_attributes_from_previous_run=False,
        )

        await db.update_attributo(
            conn,
            name=TEST_ATTRIBUTO_NAME,
            conversion_flags=AttributoConversionFlags(ignore_units=False),
            new_attributo=DBAttributo(
                # We try to change all the attributes
                name=AttributoId(str(TEST_ATTRIBUTO_NAME) + "1"),
                description=TEST_ATTRIBUTO_DESCRIPTION + "1",
                group=TEST_ATTRIBUTO_GROUP + "1",
                associated_table=AssociatedTable.RUN,
                attributo_type=AttributoTypeString(),
            ),
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        runs = await db.retrieve_runs(conn, attributi)
        assert runs[0].attributi.select_int(TEST_ATTRIBUTO_NAME) is None
        assert (
            runs[0].attributi.select_string(AttributoId(str(TEST_ATTRIBUTO_NAME) + "1"))
            is not None
        )


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
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_json(
                attributi,
                sample_ids=await db.retrieve_sample_ids(conn),
                raw_attributi={TEST_ATTRIBUTO_NAME: TEST_ATTRIBUTO_VALUE},
            ),
            keep_manual_attributes_from_previous_run=False,
        )

        await db.delete_attributo(conn, TEST_ATTRIBUTO_NAME)

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        runs = await db.retrieve_runs(conn, attributi)
        assert runs[0].attributi.select_int(TEST_ATTRIBUTO_NAME) is None


async def test_create_attributo_and_run_and_sample_for_run_then_delete_sample() -> None:
    """This is a bit of an edge case: we have an attributo that signifies the sample of a run, and we create a run and a sample, and then we delete the sample"""

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

        # Create an experiment type and a data-set. This is supposed to be deleted as well when we delete the sample
        # attributo.
        await db.create_experiment_type(
            conn, name="sample-based", experiment_attributi_names=[TEST_ATTRIBUTO_NAME]
        )

        await db.create_data_set(
            conn,
            "sample-based",
            AttributiMap.from_types_and_raw(
                attributi, [sample_id], {TEST_ATTRIBUTO_NAME: sample_id}
            ),
        )

        await db.create_run(
            conn,
            run_id=TEST_RUN_ID,
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_json(
                attributi,
                sample_ids=await db.retrieve_sample_ids(conn),
                raw_attributi={TEST_ATTRIBUTO_NAME: sample_id},
            ),
            keep_manual_attributes_from_previous_run=False,
        )

        with pytest.raises(Exception):
            # This doesn't work, because the sample is being used
            await db.delete_sample(conn, sample_id, delete_in_dependencies=False)

        # This works, because we explicitly say we want to delete it from the runs
        await db.delete_sample(conn, sample_id, delete_in_dependencies=True)

        runs = await db.retrieve_runs(conn, attributi)
        assert runs[0].attributi.select_sample_id(TEST_ATTRIBUTO_NAME) is None

        assert not await db.retrieve_data_sets(conn, [], attributi)


async def test_create_and_retrieve_file() -> None:
    db = await _get_db()

    async with db.begin() as conn:
        result = await db.create_file(
            conn,
            "name.txt",
            "my description",
            original_path=None,
            contents_location=Path(__file__).parent / "test-file-no-newlines.txt",
            deduplicate=False,
        )

        assert result.id > 0
        assert result.type_ == "text/plain"

        file_ = await db.retrieve_file(conn, result.id, with_contents=False)

        assert file_.file_name == "name.txt"
        assert file_.type_ == "text/plain"
        assert file_.size_in_bytes == 17


async def test_create_and_retrieve_file_with_deduplication() -> None:
    db = await _get_db()

    async with db.begin() as conn:
        result = await db.create_file(
            conn,
            "name.txt",
            "my description",
            original_path=None,
            contents_location=Path(__file__).parent / "test-file.txt",
            deduplicate=True,
        )

        result2 = await db.create_file(
            conn,
            "name.txt",
            "my description",
            original_path=None,
            contents_location=Path(__file__).parent / "test-file.txt",
            deduplicate=True,
        )

        assert result.id == result2.id


async def test_create_and_retrieve_file_without_deduplication() -> None:
    db = await _get_db()

    async with db.begin() as conn:
        result = await db.create_file(
            conn,
            "name.txt",
            "my description",
            original_path=None,
            contents_location=Path(__file__).parent / "test-file.txt",
            deduplicate=False,
        )

        result2 = await db.create_file(
            conn,
            "name.txt",
            "my description",
            original_path=None,
            contents_location=Path(__file__).parent / "test-file.txt",
            deduplicate=False,
        )

        assert result.id != result2.id


async def test_create_and_retrieve_experiment_types() -> None:
    db = await _get_db()

    async with db.begin() as conn:
        first_name = "a1"
        await db.create_attributo(
            conn,
            name=first_name,
            description="",
            group=ATTRIBUTO_GROUP_MANUAL,
            associated_table=AssociatedTable.RUN,
            type_=AttributoTypeInt(),
        )
        second_name = "a2"
        await db.create_attributo(
            conn,
            name=second_name,
            description="",
            group=ATTRIBUTO_GROUP_MANUAL,
            associated_table=AssociatedTable.RUN,
            type_=AttributoTypeString(),
        )

        # First try with a nonexistent attributo name
        e_type_name = "e1"
        with pytest.raises(Exception):
            await db.create_experiment_type(
                conn, e_type_name, [first_name, second_name + "lol"]
            )

        # Create the experiment type, then retrieve it, then delete it again and check if that worked
        await db.create_experiment_type(conn, e_type_name, [first_name, second_name])

        e_types = list(await db.retrieve_experiment_types(conn))
        assert len(e_types) == 1
        assert e_types[0].name == e_type_name
        assert e_types[0].attributo_names == [first_name, second_name]

        # Now delete it again
        await db.delete_experiment_type(conn, e_type_name)

        assert not await db.retrieve_experiment_types(conn)


async def test_create_and_retrieve_data_sets() -> None:
    db = await _get_db()

    async with db.begin() as conn:
        first_name = "a1"
        await db.create_attributo(
            conn,
            name=first_name,
            description="",
            group=ATTRIBUTO_GROUP_MANUAL,
            associated_table=AssociatedTable.RUN,
            type_=AttributoTypeInt(),
        )
        second_name = "a2"
        await db.create_attributo(
            conn,
            name=second_name,
            description="",
            group=ATTRIBUTO_GROUP_MANUAL,
            associated_table=AssociatedTable.RUN,
            type_=AttributoTypeString(),
        )

        # Create experiment type
        e_type_name = "e1"
        await db.create_experiment_type(conn, e_type_name, [first_name, second_name])

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        raw_attributi = {first_name: 1, second_name: "f"}
        id_ = await db.create_data_set(
            conn,
            e_type_name,
            AttributiMap.from_types_and_json(
                types=attributi,
                sample_ids=await db.retrieve_sample_ids(conn),
                raw_attributi=raw_attributi,
            ),
        )

        assert id_ > 0

        data_sets = await db.retrieve_data_sets(
            conn, await db.retrieve_sample_ids(conn), attributi
        )

        assert len(data_sets) == 1
        assert data_sets[0].id == id_
        assert data_sets[0].experiment_type == e_type_name
        assert data_sets[0].attributi.to_json() == raw_attributi

        await db.delete_data_set(conn, id_)
        assert not (
            await db.retrieve_data_sets(
                conn, await db.retrieve_sample_ids(conn), attributi
            )
        )

        with pytest.raises(Exception):
            await db.create_data_set(
                conn, e_type_name, AttributiMap.from_types_and_json(attributi, [], {})
            )


async def test_create_data_set_and_and_change_attributo_type() -> None:
    db = await _get_db()

    async with db.begin() as conn:
        first_name = "a1"
        await db.create_attributo(
            conn,
            name=first_name,
            description="",
            group=ATTRIBUTO_GROUP_MANUAL,
            associated_table=AssociatedTable.RUN,
            type_=AttributoTypeInt(),
        )
        second_name = "a2"
        await db.create_attributo(
            conn,
            name=second_name,
            description="",
            group=ATTRIBUTO_GROUP_MANUAL,
            associated_table=AssociatedTable.RUN,
            type_=AttributoTypeString(),
        )

        # Create experiment type
        e_type_name = "e1"
        await db.create_experiment_type(conn, e_type_name, [first_name, second_name])

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        raw_attributi = {first_name: 1, second_name: "f"}
        await db.create_data_set(
            conn,
            e_type_name,
            AttributiMap.from_types_and_json(
                types=attributi,
                sample_ids=await db.retrieve_sample_ids(conn),
                raw_attributi=raw_attributi,
            ),
        )

        await db.update_attributo(
            conn,
            AttributoId(first_name),
            AttributoConversionFlags(ignore_units=False),
            new_attributo=DBAttributo(
                name=AttributoId(first_name),
                description="",
                group=ATTRIBUTO_GROUP_MANUAL,
                associated_table=AssociatedTable.RUN,
                attributo_type=AttributoTypeString(),
            ),
        )

        data_sets = list(
            await db.retrieve_data_sets(
                conn,
                await db.retrieve_sample_ids(conn),
                await db.retrieve_attributi(conn, associated_table=None),
            )
        )
        assert data_sets[0].attributi.select_string(AttributoId(first_name))


async def test_create_read_delete_events() -> None:
    db = await _get_db()

    async with db.begin() as conn:
        event_text = "hihi"
        event_level = EventLogLevel.INFO
        event_id = await db.create_event(conn, event_level, EVENT_SOURCE, event_text)

        assert event_id > 0

        events = await db.retrieve_events(conn)

        assert len(events) == 1

        assert events[0].id == event_id
        assert events[0].source == EVENT_SOURCE
        assert events[0].text == event_text
        assert events[0].level == event_level

        await db.create_event(conn, event_level, EVENT_SOURCE, event_text)

        await db.delete_event(conn, event_id)

        assert len(await db.retrieve_events(conn)) == 1


async def test_create_analysis_result() -> None:
    db = await _get_db()

    async with db.begin() as conn:
        attributo_name = "a1"
        await db.create_attributo(
            conn,
            name=attributo_name,
            description="",
            group=ATTRIBUTO_GROUP_MANUAL,
            associated_table=AssociatedTable.RUN,
            type_=AttributoTypeString(),
        )

        # Create experiment type
        e_type_name = "e1"
        await db.create_experiment_type(conn, e_type_name, [attributo_name])

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        raw_attributi: JsonAttributiMap = {attributo_name: "foo"}
        data_set_id = await db.create_data_set(
            conn,
            e_type_name,
            AttributiMap.from_types_and_json(
                types=attributi,
                sample_ids=await db.retrieve_sample_ids(conn),
                raw_attributi=raw_attributi,
            ),
        )

        result_id = await db.create_cfel_analysis_result(
            conn,
            DBCFELAnalysisResult(
                id=None,
                directory_name="/tmp",
                data_set_id=data_set_id,
                resolution="1.0",
                rsplit=1.0,
                cchalf=1.0,
                ccstar=1.0,
                snr=1.0,
                completeness=1.0,
                multiplicity=1.0,
                total_measurements=100,
                unique_reflections=50,
                num_patterns=1,
                num_hits=0,
                indexed_patterns=10,
                indexed_crystals=100,
                crystfel_version="1.0",
                ccstar_rsplit=1.0,
                created=datetime.datetime.utcnow(),
                files=[],
            ),
            [],
        )

        analysis_results = await db.retrieve_cfel_analysis_results(conn)

        assert len(analysis_results) == 1
        assert analysis_results[0].id == result_id
        # Smoke test
        assert analysis_results[0].crystfel_version == "1.0"

        await db.clear_cfel_analysis_results(conn)

        assert not await db.retrieve_cfel_analysis_results(conn)


async def test_create_and_retrieve_attributo_no_runs() -> None:
    db = await _get_db()

    async with db.begin() as conn:
        await db.retrieve_bulk_run_attributi(
            conn,
            await db.retrieve_attributi(conn, associated_table=AssociatedTable.RUN),
            [],
        )


async def test_create_and_retrieve_attributo_two_runs() -> None:
    db = await _get_db()

    async with db.begin() as conn:
        await db.create_attributo(
            conn,
            TEST_ATTRIBUTO_NAME,
            TEST_ATTRIBUTO_DESCRIPTION,
            TEST_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            AttributoTypeString(),
        )
        await db.create_attributo(
            conn,
            TEST_SECOND_ATTRIBUTO_NAME,
            TEST_ATTRIBUTO_DESCRIPTION,
            TEST_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(
            conn, associated_table=AssociatedTable.RUN
        )

        run_data: Dict[int, JsonAttributiMap] = {
            TEST_RUN_ID: {
                # Important: here, we have "foo" and 1, below we have something else for "foo", but the same for 1
                TEST_ATTRIBUTO_NAME: "foo",
                TEST_SECOND_ATTRIBUTO_NAME: 1,
            },
            TEST_RUN_ID
            + 1: {
                TEST_ATTRIBUTO_NAME: "bar",
                TEST_SECOND_ATTRIBUTO_NAME: 1,
            },
        }

        for run_id, data in run_data.items():
            await db.create_run(
                conn,
                run_id=run_id,
                attributi=attributi,
                attributi_map=AttributiMap.from_types_and_json(
                    attributi, sample_ids=[], raw_attributi=data
                ),
                keep_manual_attributes_from_previous_run=False,
            )

        bulk_attributi = await db.retrieve_bulk_run_attributi(
            conn,
            attributi=attributi,
            run_ids=[TEST_RUN_ID, TEST_RUN_ID + 1],
        )

        assert bulk_attributi == {
            TEST_ATTRIBUTO_NAME: {"foo", "bar"},
            TEST_SECOND_ATTRIBUTO_NAME: {1},
        }

        new_test_attributo = "baz"
        await db.update_bulk_run_attributi(
            conn,
            attributi,
            run_ids={TEST_RUN_ID, TEST_RUN_ID + 1},
            attributi_values=AttributiMap.from_types_and_raw(
                attributi,
                [],
                {
                    # We only store one new attributo for both runs and see if only that gets updated
                    TEST_ATTRIBUTO_NAME: new_test_attributo,
                },
            ),
        )

        assert (  # type: ignore
            await db.retrieve_run(conn, TEST_RUN_ID, attributi)
        ).attributi.select_string(TEST_ATTRIBUTO_NAME) == new_test_attributo

        assert (  # type: ignore
            await db.retrieve_run(conn, TEST_RUN_ID + 1, attributi)
        ).attributi.select_string(TEST_ATTRIBUTO_NAME) == new_test_attributo


async def test_create_workbook() -> None:
    db = await _get_db()

    async with db.begin() as conn:
        await db.create_attributo(
            conn,
            TEST_ATTRIBUTO_NAME,
            TEST_ATTRIBUTO_DESCRIPTION,
            TEST_ATTRIBUTO_GROUP,
            AssociatedTable.SAMPLE,
            AttributoTypeString(),
        )
        await db.create_attributo(
            conn,
            TEST_SECOND_ATTRIBUTO_NAME,
            TEST_ATTRIBUTO_DESCRIPTION,
            TEST_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            AttributoTypeString(),
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)
        await db.create_sample(
            conn,
            "first sample",
            AttributiMap.from_types_and_raw(
                attributi,
                sample_ids=[],
                raw_attributi={TEST_ATTRIBUTO_NAME: "foo"},
            ),
        )

        await db.create_run(
            conn,
            run_id=1,
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_raw(
                types=attributi,
                sample_ids=[],
                raw_attributi={TEST_SECOND_ATTRIBUTO_NAME: "foo"},
            ),
            keep_manual_attributes_from_previous_run=False,
        )

        wb = (await create_workbook(db, conn, with_events=True)).workbook

        attributi_wb = wb["Attributi"]
        assert attributi_wb is not None

        # Man am I tired of example-based testing
        assert attributi_wb["A1"].value == "Table"
        assert attributi_wb["B1"].value == "Name"

        assert attributi_wb["A2"].value == "Run"
        assert attributi_wb["B2"].value == "started"

        runs_wb = wb["Runs"]
        assert runs_wb is not None

        assert runs_wb["A1"].value == "ID"
        assert runs_wb["B1"].value == ATTRIBUTO_STARTED
        assert runs_wb["C1"].value == ATTRIBUTO_STOPPED
        assert runs_wb["D1"].value == TEST_SECOND_ATTRIBUTO_NAME

        assert runs_wb["D2"].value == "foo"

        sample_wb = wb["Samples"]
        assert sample_wb is not None

        assert sample_wb["A1"].value == "Name"
        assert sample_wb["B1"].value == TEST_ATTRIBUTO_NAME

        assert sample_wb["A2"].value == "first sample"
        assert sample_wb["B2"].value == "foo"
