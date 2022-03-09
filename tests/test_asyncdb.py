import datetime
from pathlib import Path

import pytest

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.attributi import AttributoConversionFlags
from amarcord.db.attributi_map import AttributiMap, JsonAttributiMap
from amarcord.db.attributo_type import (
    AttributoTypeInt,
    AttributoTypeString,
    AttributoTypeSample,
)
from amarcord.db.cfel_analysis_result import DBCFELAnalysisResult
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.dbcontext import CreationMode
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.tables import create_tables_from_metadata

EVENT_SOURCE = "P11User"

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
            attributi=AttributiMap.from_types_and_json(
                attributi,
                sample_ids=await db.retrieve_sample_ids(conn),
                raw_attributi={TEST_ATTRIBUTO_NAME: TEST_ATTRIBUTO_VALUE},
            ),
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


async def test_create_and_retrieve_file() -> None:
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

        assert result.id > 0
        assert result.type_ == "text/plain"

        (
            file_name,
            mime_type,
            _contents,
            file_size_in_bytes,
        ) = await db.retrieve_file(conn, result.id)

        assert file_name == "name.txt"
        assert mime_type == "text/plain"
        assert file_size_in_bytes == 17


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
            group="manual",
            associated_table=AssociatedTable.RUN,
            type_=AttributoTypeInt(),
        )
        second_name = "a2"
        await db.create_attributo(
            conn,
            name=second_name,
            description="",
            group="manual",
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
            group="manual",
            associated_table=AssociatedTable.RUN,
            type_=AttributoTypeInt(),
        )
        second_name = "a2"
        await db.create_attributo(
            conn,
            name=second_name,
            description="",
            group="manual",
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
            group="manual",
            associated_table=AssociatedTable.RUN,
            type_=AttributoTypeInt(),
        )
        second_name = "a2"
        await db.create_attributo(
            conn,
            name=second_name,
            description="",
            group="manual",
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
            first_name,
            AttributoConversionFlags(ignore_units=False),
            new_attributo=DBAttributo(
                name=first_name,
                description="",
                group="manual",
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
        assert data_sets[0].attributi.select_string(first_name)


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
            group="manual",
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
