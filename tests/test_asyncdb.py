import datetime
from pathlib import Path
from typing import Final

import numpy
import pytest
from sqlalchemy.exc import StatementError

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import ATTRIBUTO_GROUP_MANUAL
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.asyncdb import create_workbook
from amarcord.db.attributi import ATTRIBUTO_STARTED
from amarcord.db.attributi import ATTRIBUTO_STOPPED
from amarcord.db.attributi import AttributoConversionFlags
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.attributi_map import JsonAttributiMap
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoTypeChemical
from amarcord.db.attributo_type import AttributoTypeDecimal
from amarcord.db.attributo_type import AttributoTypeInt
from amarcord.db.attributo_type import AttributoTypeString
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.indexing_result import DBIndexingResultInput
from amarcord.db.tables import create_tables_from_metadata
from amarcord.db.user_configuration import UserConfiguration

_EVENT_SOURCE: Final = "P11User"
_TEST_DB_URL: Final = "sqlite+aiosqlite://"
_TEST_ATTRIBUTO_VALUE: Final = 3
_TEST_SECOND_ATTRIBUTO_VALUE: Final = 4
_TEST_CHEMICAL_NAME: Final = "chemicalname"
_TEST_RUN_ID: Final = 1
_TEST_ATTRIBUTO_GROUP: Final = "testgroup"
_TEST_ATTRIBUTO_DESCRIPTION: Final = "testdescription"
_TEST_ATTRIBUTO_NAME: Final = AttributoId("testname")
_TEST_SECOND_ATTRIBUTO_NAME: Final = AttributoId("testname1")


async def _get_db(use_sqlalchemy_default_json_serializer: bool = False) -> AsyncDB:
    context = AsyncDBContext(
        _TEST_DB_URL,
        use_sqlalchemy_default_json_serializer=use_sqlalchemy_default_json_serializer,
    )
    db = AsyncDB(context, create_tables_from_metadata(context.metadata))
    await db.migrate()
    return db


async def test_create_and_retrieve_attributo() -> None:
    """Just a really simple test: create a single attributo for a chemical, then retrieve it"""
    db = await _get_db()

    async with db.begin() as conn:
        await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.CHEMICAL,
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(conn, AssociatedTable.CHEMICAL)
        assert len(attributi) == 1
        assert attributi[0].attributo_type == AttributoTypeInt()
        assert attributi[0].name == _TEST_ATTRIBUTO_NAME
        assert attributi[0].description == _TEST_ATTRIBUTO_DESCRIPTION
        assert attributi[0].group == _TEST_ATTRIBUTO_GROUP

        # Check if the filter for the table works: there should be no attributi for runs
        assert not [
            a
            for a in await db.retrieve_attributi(conn, AssociatedTable.RUN)
            if a.name == _TEST_ATTRIBUTO_NAME
        ]


async def test_create_chemical_attributo_then_change_to_run_attributo() -> None:
    db = await _get_db()

    async with db.begin() as conn:
        # Create our test attributo
        await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.CHEMICAL,
            AttributoTypeInt(),
        )

        # Add the attributo to a test chemical
        chemical_id_with_attributo = await db.create_chemical(
            conn,
            _TEST_CHEMICAL_NAME,
            AttributiMap.from_types_and_json(
                await db.retrieve_attributi(conn, None),
                chemical_ids=[],
                raw_attributi={_TEST_ATTRIBUTO_NAME: 3},
            ),
        )
        chemical_id_without_attributo = await db.create_chemical(
            conn,
            _TEST_CHEMICAL_NAME,
            AttributiMap.from_types_and_json(
                await db.retrieve_attributi(conn, None),
                chemical_ids=[],
                raw_attributi={},
            ),
        )

        # Now change the type to run
        await db.update_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            AttributoConversionFlags(ignore_units=False),
            DBAttributo(
                _TEST_ATTRIBUTO_NAME,
                _TEST_ATTRIBUTO_DESCRIPTION,
                _TEST_ATTRIBUTO_GROUP,
                AssociatedTable.RUN,
                AttributoTypeInt(),
            ),
        )

        # chemical shouldn't contain the attributo anymore
        chemicals = await db.retrieve_chemicals(
            conn, await db.retrieve_attributi(conn, None)
        )

        assert chemicals[0].id == chemical_id_with_attributo
        assert chemicals[0].attributi.select_int(_TEST_ATTRIBUTO_NAME) is None
        assert chemicals[1].id == chemical_id_without_attributo
        assert chemicals[1].attributi.select_int(_TEST_ATTRIBUTO_NAME) is None


async def test_create_run_attributo_then_change_to_chemical_attributo() -> None:
    db = await _get_db()

    async with db.begin() as conn:
        # Create our test attributo
        await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            AttributoTypeInt(),
        )

        # Add the attributo to a test chemical
        await db.create_run(
            conn,
            _TEST_RUN_ID,
            await db.retrieve_attributi(conn, None),
            AttributiMap.from_types_and_json(
                await db.retrieve_attributi(conn, None),
                chemical_ids=[],
                raw_attributi={_TEST_ATTRIBUTO_NAME: 3},
            ),
            keep_manual_attributes_from_previous_run=False,
        )
        await db.create_run(
            conn,
            _TEST_RUN_ID + 1,
            await db.retrieve_attributi(conn, None),
            AttributiMap.from_types_and_json(
                await db.retrieve_attributi(conn, None),
                chemical_ids=[],
                raw_attributi={},
            ),
            keep_manual_attributes_from_previous_run=False,
        )

        # Now change the type to run
        await db.update_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            AttributoConversionFlags(ignore_units=False),
            DBAttributo(
                _TEST_ATTRIBUTO_NAME,
                _TEST_ATTRIBUTO_DESCRIPTION,
                _TEST_ATTRIBUTO_GROUP,
                AssociatedTable.CHEMICAL,
                AttributoTypeInt(),
            ),
        )

        # chemical shouldn't contain the attributo anymore
        runs = await db.retrieve_runs(conn, await db.retrieve_attributi(conn, None))

        assert runs[0].id == _TEST_RUN_ID + 1
        assert runs[0].attributi.select_int(_TEST_ATTRIBUTO_NAME) is None
        assert runs[1].id == _TEST_RUN_ID
        assert runs[1].attributi.select_int(_TEST_ATTRIBUTO_NAME) is None


async def test_create_and_delete_unused_attributo() -> None:
    """Create an attributo, then delete it again"""

    db = await _get_db()

    async with db.begin() as conn:
        await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.CHEMICAL,
            AttributoTypeInt(),
        )

        await db.delete_attributo(conn, _TEST_ATTRIBUTO_NAME)

        assert not [
            a
            for a in await db.retrieve_attributi(conn, associated_table=None)
            if a.name == _TEST_ATTRIBUTO_NAME
        ]


async def test_create_and_retrieve_chemical_with_nan() -> None:
    """Create an attributo, then a chemical, and then retrieve that. NaN value is used"""

    db = await _get_db()

    async with db.begin() as conn:
        await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.CHEMICAL,
            AttributoTypeDecimal(),
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        with pytest.raises(StatementError):
            await db.create_chemical(
                conn,
                name=_TEST_CHEMICAL_NAME,
                attributi=AttributiMap.from_types_and_json(
                    attributi,
                    chemical_ids=[],
                    raw_attributi={_TEST_ATTRIBUTO_NAME: numpy.NAN},
                ),
            )

    db_default_json_serializer = await _get_db(
        use_sqlalchemy_default_json_serializer=True
    )

    async with db_default_json_serializer.begin() as conn:
        await db_default_json_serializer.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.CHEMICAL,
            AttributoTypeDecimal(),
        )

        attributi = await db_default_json_serializer.retrieve_attributi(
            conn, associated_table=None
        )

        try:
            await db_default_json_serializer.create_chemical(
                conn,
                name=_TEST_CHEMICAL_NAME,
                attributi=AttributiMap.from_types_and_json(
                    attributi,
                    chemical_ids=[],
                    raw_attributi={_TEST_ATTRIBUTO_NAME: numpy.NAN},
                ),
            )
        except Exception:
            pytest.fail("Unexpected excepetion")


async def test_create_and_retrieve_chemical() -> None:
    """Create an attributo, then a chemical, and then retrieve that"""

    db = await _get_db()

    async with db.begin() as conn:
        await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.CHEMICAL,
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        chemical_id = await db.create_chemical(
            conn,
            name=_TEST_CHEMICAL_NAME,
            attributi=AttributiMap.from_types_and_json(
                attributi,
                chemical_ids=[],
                raw_attributi={_TEST_ATTRIBUTO_NAME: _TEST_ATTRIBUTO_VALUE},
            ),
        )

        assert isinstance(chemical_id, int)

        chemicals = await db.retrieve_chemicals(conn, attributi)

        assert len(chemicals) == 1
        assert chemicals[0].name == _TEST_CHEMICAL_NAME
        assert chemicals[0].id == chemical_id
        assert not chemicals[0].files
        assert (
            chemicals[0].attributi.select_int_unsafe(_TEST_ATTRIBUTO_NAME)
            == _TEST_ATTRIBUTO_VALUE
        )

        chemical = await db.retrieve_chemical(conn, chemical_id, attributi)
        assert chemical is not None
        assert chemical.id == chemical_id
        assert chemical.name == _TEST_CHEMICAL_NAME


async def test_create_and_update_chemical() -> None:
    """Create two attributi, then a chemical, and then update that"""

    db = await _get_db()

    async with db.begin() as conn:
        await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.CHEMICAL,
            AttributoTypeInt(),
        )
        await db.create_attributo(
            conn,
            _TEST_SECOND_ATTRIBUTO_NAME,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.CHEMICAL,
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        chemical_id = await db.create_chemical(
            conn,
            name=_TEST_CHEMICAL_NAME,
            attributi=AttributiMap.from_types_and_json(
                attributi,
                chemical_ids=[],
                raw_attributi={
                    _TEST_ATTRIBUTO_NAME: _TEST_ATTRIBUTO_VALUE,
                    _TEST_SECOND_ATTRIBUTO_NAME: _TEST_SECOND_ATTRIBUTO_VALUE,
                },
            ),
        )

        await db.update_chemical(
            conn,
            chemical_id,
            name=_TEST_CHEMICAL_NAME + "1",
            attributi=AttributiMap.from_types_and_json(
                attributi,
                chemical_ids=[],
                raw_attributi={
                    _TEST_ATTRIBUTO_NAME: _TEST_ATTRIBUTO_VALUE + 1,
                    _TEST_SECOND_ATTRIBUTO_NAME: _TEST_SECOND_ATTRIBUTO_VALUE,
                },
            ),
        )

        chemicals = await db.retrieve_chemicals(conn, attributi)

        assert len(chemicals) == 1
        assert chemicals[0].name == _TEST_CHEMICAL_NAME + "1"
        assert chemicals[0].id == chemical_id
        assert (
            chemicals[0].attributi.select_int_unsafe(_TEST_ATTRIBUTO_NAME)
            == _TEST_ATTRIBUTO_VALUE + 1
        )
        assert (
            chemicals[0].attributi.select_int_unsafe(_TEST_SECOND_ATTRIBUTO_NAME)
            == _TEST_SECOND_ATTRIBUTO_VALUE
        )


async def test_create_and_delete_chemical() -> None:
    """Create an attributo, then a chemical, delete the chemical again"""

    db = await _get_db()

    async with db.begin() as conn:
        await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.CHEMICAL,
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        chemical_id = await db.create_chemical(
            conn,
            name=_TEST_CHEMICAL_NAME,
            attributi=AttributiMap.from_types_and_json(
                attributi,
                chemical_ids=[],
                raw_attributi={_TEST_ATTRIBUTO_NAME: _TEST_ATTRIBUTO_VALUE},
            ),
        )

        await db.delete_chemical(conn, chemical_id, delete_in_dependencies=True)
        assert not await db.retrieve_chemicals(conn, attributi)


async def test_create_attributo_and_chemical_then_change_attributo() -> None:
    """Create an attributo, then a chemical, then change the attributo. Should propagate to the chemical"""

    db = await _get_db()

    async with db.begin() as conn:
        await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.CHEMICAL,
            # We create the attributo as int, then convert to string
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        await db.create_chemical(
            conn,
            name=_TEST_CHEMICAL_NAME,
            attributi=AttributiMap.from_types_and_json(
                attributi,
                chemical_ids=[],
                raw_attributi={_TEST_ATTRIBUTO_NAME: _TEST_ATTRIBUTO_VALUE},
            ),
        )

        await db.update_attributo(
            conn,
            name=_TEST_ATTRIBUTO_NAME,
            conversion_flags=AttributoConversionFlags(ignore_units=False),
            new_attributo=DBAttributo(
                # We try to change all the attributes
                name=AttributoId(str(_TEST_ATTRIBUTO_NAME) + "1"),
                description=_TEST_ATTRIBUTO_DESCRIPTION + "1",
                group=_TEST_ATTRIBUTO_GROUP + "1",
                associated_table=AssociatedTable.CHEMICAL,
                attributo_type=AttributoTypeString(),
            ),
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)
        test_attributi = [a for a in attributi if a.name == _TEST_ATTRIBUTO_NAME + "1"]

        assert len(test_attributi) == 1
        assert test_attributi[0].name == _TEST_ATTRIBUTO_NAME + "1"
        assert test_attributi[0].description == _TEST_ATTRIBUTO_DESCRIPTION + "1"
        assert test_attributi[0].group == _TEST_ATTRIBUTO_GROUP + "1"

        chemicals = await db.retrieve_chemicals(conn, attributi)
        assert chemicals[0].attributi.select_int(_TEST_ATTRIBUTO_NAME) is None
        assert (
            chemicals[0].attributi.select_string(
                AttributoId(str(_TEST_ATTRIBUTO_NAME) + "1")
            )
            is not None
        )


async def test_create_attributo_and_chemical_then_delete_attributo() -> None:
    """Create an attributo, then a chemical, then delete the attributo. Should propagate to the chemical"""

    db = await _get_db()

    async with db.begin() as conn:
        await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.CHEMICAL,
            # We create the attributo as int, then convert to string
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        await db.create_chemical(
            conn,
            name=_TEST_CHEMICAL_NAME,
            attributi=AttributiMap.from_types_and_json(
                attributi,
                chemical_ids=[],
                raw_attributi={_TEST_ATTRIBUTO_NAME: _TEST_ATTRIBUTO_VALUE},
            ),
        )

        await db.delete_attributo(conn, _TEST_ATTRIBUTO_NAME)

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        chemicals = await db.retrieve_chemicals(conn, attributi)
        assert chemicals[0].attributi.select_int(_TEST_ATTRIBUTO_NAME) is None


async def test_create_and_retrieve_runs() -> None:
    """Create an attributo, then a run, and then retrieve that"""

    db = await _get_db()

    async with db.begin() as conn:
        await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        await db.create_run(
            conn,
            run_id=_TEST_RUN_ID,
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_json(
                attributi,
                chemical_ids=await db.retrieve_chemical_ids(conn),
                raw_attributi={_TEST_ATTRIBUTO_NAME: _TEST_ATTRIBUTO_VALUE},
            ),
            keep_manual_attributes_from_previous_run=False,
        )

        runs = await db.retrieve_runs(conn, attributi)

        assert len(runs) == 1
        assert runs[0].id == _TEST_RUN_ID
        assert not runs[0].files
        assert (
            runs[0].attributi.select_int_unsafe(_TEST_ATTRIBUTO_NAME)
            == _TEST_ATTRIBUTO_VALUE
        )


async def test_create_and_retrieve_run() -> None:
    """Create an attributo, then a run, and then retrieve just that run"""

    db = await _get_db()

    async with db.begin() as conn:
        await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        await db.create_run(
            conn,
            run_id=_TEST_RUN_ID,
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_json(
                attributi,
                chemical_ids=await db.retrieve_chemical_ids(conn),
                raw_attributi={_TEST_ATTRIBUTO_NAME: _TEST_ATTRIBUTO_VALUE},
            ),
            keep_manual_attributes_from_previous_run=False,
        )

        run = await db.retrieve_run(conn, _TEST_RUN_ID, attributi)

        assert run is not None
        assert run.id == _TEST_RUN_ID
        assert not run.files
        assert (
            run.attributi.select_int_unsafe(_TEST_ATTRIBUTO_NAME)
            == _TEST_ATTRIBUTO_VALUE
        )

        run = await db.retrieve_latest_run(conn, attributi)

        assert run is not None
        assert run.id == _TEST_RUN_ID
        assert not run.files
        assert (
            run.attributi.select_int_unsafe(_TEST_ATTRIBUTO_NAME)
            == _TEST_ATTRIBUTO_VALUE
        )


async def test_create_run_and_then_next_run_using_previous_attributi() -> None:
    """Create a run with some attributi, then another run and test the "keep attributi from previous" feature"""

    db = await _get_db()

    async with db.begin() as conn:
        # The mechanism to copy over attributes from the previous run is hard-coded to "manual" attributi, so let's
        # create one that's manual and one that isn't
        await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            _TEST_ATTRIBUTO_DESCRIPTION,
            ATTRIBUTO_GROUP_MANUAL,
            AssociatedTable.RUN,
            AttributoTypeInt(),
        )
        second_test_attribute = AttributoId(str(_TEST_ATTRIBUTO_NAME) + "2")
        await db.create_attributo(
            conn,
            second_test_attribute,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        await db.create_run(
            conn,
            run_id=_TEST_RUN_ID,
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_json(
                attributi,
                chemical_ids=await db.retrieve_chemical_ids(conn),
                # Only one of the two attributi here (the manual one)
                raw_attributi={_TEST_ATTRIBUTO_NAME: _TEST_ATTRIBUTO_VALUE},
            ),
            # Flag doesn't matter if it's just one run
            keep_manual_attributes_from_previous_run=False,
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        second_test_attribute_value = _TEST_ATTRIBUTO_VALUE + 1
        await db.create_run(
            conn,
            # Next Run ID
            run_id=_TEST_RUN_ID + 1,
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_json(
                attributi,
                chemical_ids=await db.retrieve_chemical_ids(conn),
                # The other attributo with a different value
                raw_attributi={second_test_attribute: second_test_attribute_value},
            ),
            # Keep previous (manual) attributi
            keep_manual_attributes_from_previous_run=True,
        )

        runs = await db.retrieve_runs(conn, attributi)
        assert len(runs) == 2
        # Assume ordering by ID descending
        assert runs[0].id == _TEST_RUN_ID + 1
        assert (
            runs[0].attributi.select_int(_TEST_ATTRIBUTO_NAME) == _TEST_ATTRIBUTO_VALUE
        )
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
            _TEST_ATTRIBUTO_NAME,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            # We create the attributo as int, then convert to string
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        await db.create_run(
            conn,
            run_id=_TEST_RUN_ID,
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_json(
                attributi,
                chemical_ids=await db.retrieve_chemical_ids(conn),
                raw_attributi={_TEST_ATTRIBUTO_NAME: _TEST_ATTRIBUTO_VALUE},
            ),
            keep_manual_attributes_from_previous_run=False,
        )

        await db.update_attributo(
            conn,
            name=_TEST_ATTRIBUTO_NAME,
            conversion_flags=AttributoConversionFlags(ignore_units=False),
            new_attributo=DBAttributo(
                # We try to change all the attributes
                name=AttributoId(str(_TEST_ATTRIBUTO_NAME) + "1"),
                description=_TEST_ATTRIBUTO_DESCRIPTION + "1",
                group=_TEST_ATTRIBUTO_GROUP + "1",
                associated_table=AssociatedTable.RUN,
                attributo_type=AttributoTypeString(),
            ),
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        runs = await db.retrieve_runs(conn, attributi)
        assert runs[0].attributi.select_int(_TEST_ATTRIBUTO_NAME) is None
        assert (
            runs[0].attributi.select_string(
                AttributoId(str(_TEST_ATTRIBUTO_NAME) + "1")
            )
            is not None
        )


async def test_create_attributo_and_run_then_delete_attributo() -> None:
    """Create an attributo, then a run, then change the attributo. Should propagate to the run"""

    db = await _get_db()

    async with db.begin() as conn:
        await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            # We create the attributo as int, then convert to string
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        await db.create_run(
            conn,
            run_id=_TEST_RUN_ID,
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_json(
                attributi,
                chemical_ids=await db.retrieve_chemical_ids(conn),
                raw_attributi={_TEST_ATTRIBUTO_NAME: _TEST_ATTRIBUTO_VALUE},
            ),
            keep_manual_attributes_from_previous_run=False,
        )

        await db.delete_attributo(conn, _TEST_ATTRIBUTO_NAME)

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        runs = await db.retrieve_runs(conn, attributi)
        assert runs[0].attributi.select_int(_TEST_ATTRIBUTO_NAME) is None


async def test_create_attributo_and_run_and_chemical_for_run_then_delete_chemical() -> None:
    """This is a bit of an edge case: we have an attributo that signifies the chemical of a run, and we create a run and a chemical, and then we delete the chemical"""

    db = await _get_db()

    async with db.begin() as conn:
        await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            AttributoTypeChemical(),
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        chemical_id = await db.create_chemical(
            conn,
            name=_TEST_CHEMICAL_NAME,
            attributi=AttributiMap.from_types_and_json(
                attributi, chemical_ids=[], raw_attributi={}
            ),
        )

        # Create an experiment type and a data-set. This is supposed to be deleted as well when we delete the chemical
        # attributo.
        et_id = await db.create_experiment_type(
            conn,
            name="chemical-based",
            experiment_attributi_names=[_TEST_ATTRIBUTO_NAME],
        )

        await db.create_data_set(
            conn,
            et_id,
            AttributiMap.from_types_and_raw(
                attributi, [chemical_id], {_TEST_ATTRIBUTO_NAME: chemical_id}
            ),
        )

        await db.create_run(
            conn,
            run_id=_TEST_RUN_ID,
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_json(
                attributi,
                chemical_ids=await db.retrieve_chemical_ids(conn),
                raw_attributi={_TEST_ATTRIBUTO_NAME: chemical_id},
            ),
            keep_manual_attributes_from_previous_run=False,
        )

        with pytest.raises(Exception):
            # This doesn't work, because the chemical is being used
            await db.delete_chemical(conn, chemical_id, delete_in_dependencies=False)

        # This works, because we explicitly say we want to delete it from the runs
        await db.delete_chemical(conn, chemical_id, delete_in_dependencies=True)

        runs = await db.retrieve_runs(conn, attributi)
        assert runs[0].attributi.select_chemical_id(_TEST_ATTRIBUTO_NAME) is None

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
        et_id = await db.create_experiment_type(
            conn, e_type_name, [first_name, second_name]
        )

        e_types = list(await db.retrieve_experiment_types(conn))
        assert len(e_types) == 1
        assert e_types[0].name == e_type_name
        assert e_types[0].attributi_names == [first_name, second_name]

        # Now delete it again
        await db.delete_experiment_type(conn, et_id)

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
        et_id = await db.create_experiment_type(
            conn, e_type_name, [first_name, second_name]
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        raw_attributi = {first_name: 1, second_name: "f"}
        id_ = await db.create_data_set(
            conn,
            et_id,
            AttributiMap.from_types_and_json(
                types=attributi,
                chemical_ids=await db.retrieve_chemical_ids(conn),
                raw_attributi=raw_attributi,
            ),
        )

        assert id_ > 0

        data_sets = await db.retrieve_data_sets(
            conn, await db.retrieve_chemical_ids(conn), attributi
        )

        assert len(data_sets) == 1
        assert data_sets[0].id == id_
        assert data_sets[0].experiment_type_id == et_id
        assert data_sets[0].attributi.to_json() == raw_attributi

        await db.delete_data_set(conn, id_)
        assert not (
            await db.retrieve_data_sets(
                conn, await db.retrieve_chemical_ids(conn), attributi
            )
        )

        with pytest.raises(Exception):
            await db.create_data_set(
                conn, et_id, AttributiMap.from_types_and_json(attributi, [], {})
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
        et_id = await db.create_experiment_type(
            conn, e_type_name, [first_name, second_name]
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)

        raw_attributi = {first_name: 1, second_name: "f"}
        await db.create_data_set(
            conn,
            et_id,
            AttributiMap.from_types_and_json(
                types=attributi,
                chemical_ids=await db.retrieve_chemical_ids(conn),
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
                await db.retrieve_chemical_ids(conn),
                await db.retrieve_attributi(conn, associated_table=None),
            )
        )
        assert data_sets[0].attributi.select_string(AttributoId(first_name))


async def test_create_read_delete_events() -> None:
    db = await _get_db()

    async with db.begin() as conn:
        event_text = "hihi"
        event_level = EventLogLevel.INFO
        event_id = await db.create_event(conn, event_level, _EVENT_SOURCE, event_text)

        assert event_id > 0

        events = await db.retrieve_events(conn)

        assert len(events) == 1

        assert events[0].id == event_id
        assert events[0].source == _EVENT_SOURCE
        assert events[0].text == event_text
        assert events[0].level == event_level

        await db.create_event(conn, event_level, _EVENT_SOURCE, event_text)

        await db.delete_event(conn, event_id)

        assert len(await db.retrieve_events(conn)) == 1


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
            _TEST_ATTRIBUTO_NAME,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            AttributoTypeString(),
        )
        await db.create_attributo(
            conn,
            _TEST_SECOND_ATTRIBUTO_NAME,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(
            conn, associated_table=AssociatedTable.RUN
        )

        run_data: dict[int, JsonAttributiMap] = {
            _TEST_RUN_ID: {
                # Important: here, we have "foo" and 1, below we have something else for "foo", but the same for 1
                _TEST_ATTRIBUTO_NAME: "foo",
                _TEST_SECOND_ATTRIBUTO_NAME: 1,
            },
            _TEST_RUN_ID
            + 1: {
                _TEST_ATTRIBUTO_NAME: "bar",
                _TEST_SECOND_ATTRIBUTO_NAME: 1,
            },
        }

        for run_id, data in run_data.items():
            await db.create_run(
                conn,
                run_id=run_id,
                attributi=attributi,
                attributi_map=AttributiMap.from_types_and_json(
                    attributi, chemical_ids=[], raw_attributi=data
                ),
                keep_manual_attributes_from_previous_run=False,
            )

        bulk_attributi = await db.retrieve_bulk_run_attributi(
            conn,
            attributi=attributi,
            run_ids=[_TEST_RUN_ID, _TEST_RUN_ID + 1],
        )

        assert bulk_attributi == {
            _TEST_ATTRIBUTO_NAME: {"foo", "bar"},
            _TEST_SECOND_ATTRIBUTO_NAME: {1},
        }

        new_test_attributo = "baz"
        await db.update_bulk_run_attributi(
            conn,
            attributi,
            run_ids={_TEST_RUN_ID, _TEST_RUN_ID + 1},
            attributi_values=AttributiMap.from_types_and_raw(
                attributi,
                [],
                {
                    # We only store one new attributo for both runs and see if only that gets updated
                    _TEST_ATTRIBUTO_NAME: new_test_attributo,
                },
            ),
        )

        assert (  # type: ignore
            await db.retrieve_run(conn, _TEST_RUN_ID, attributi)
        ).attributi.select_string(_TEST_ATTRIBUTO_NAME) == new_test_attributo

        assert (  # type: ignore
            await db.retrieve_run(conn, _TEST_RUN_ID + 1, attributi)
        ).attributi.select_string(_TEST_ATTRIBUTO_NAME) == new_test_attributo


async def test_create_workbook() -> None:
    db = await _get_db()

    async with db.begin() as conn:
        await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.CHEMICAL,
            AttributoTypeString(),
        )
        await db.create_attributo(
            conn,
            _TEST_SECOND_ATTRIBUTO_NAME,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            AttributoTypeString(),
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)
        await db.create_chemical(
            conn,
            "first chemical",
            AttributiMap.from_types_and_raw(
                attributi,
                chemical_ids=[],
                raw_attributi={_TEST_ATTRIBUTO_NAME: "foo"},
            ),
        )

        await db.create_run(
            conn,
            run_id=1,
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_raw(
                types=attributi,
                chemical_ids=[],
                raw_attributi={_TEST_SECOND_ATTRIBUTO_NAME: "foo"},
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
        assert runs_wb["D1"].value == _TEST_SECOND_ATTRIBUTO_NAME

        assert runs_wb["D2"].value == "foo"

        chemical_wb = wb["chemicals"]
        assert chemical_wb is not None

        assert chemical_wb["A1"].value == "Name"
        assert chemical_wb["B1"].value == _TEST_ATTRIBUTO_NAME

        assert chemical_wb["A2"].value == "first chemical"
        assert chemical_wb["B2"].value == "foo"


async def test_retrieve_and_update_configuration() -> None:
    db = await _get_db()

    async with db.begin() as conn:
        assert (await db.retrieve_configuration(conn)).auto_pilot
        await db.update_configuration(
            conn, UserConfiguration(auto_pilot=False, use_online_crystfel=False)
        )
        assert not (await db.retrieve_configuration(conn)).auto_pilot


async def test_create_analysis_result() -> None:
    db = await _get_db()

    async with db.begin() as conn:
        attributi = await db.retrieve_attributi(conn, associated_table=None)

        await db.create_run(
            conn,
            run_id=1,
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_json(
                attributi, chemical_ids=[], raw_attributi={}
            ),
            keep_manual_attributes_from_previous_run=False,
        )

        indexing_result_id = await db.create_indexing_result(
            conn,
            DBIndexingResultInput(
                created=datetime.datetime.utcnow(),
                run_id=1,
                frames=0,
                hits=0,
                not_indexed_frames=0,
                runtime_status=None,
            ),
        )

        assert indexing_result_id is not None
