import datetime
from pathlib import Path
from typing import Final

import numpy
import pytest

from amarcord.amici.crystfel.util import parse_cell_description
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import ATTRIBUTO_GROUP_MANUAL
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.attributi import AttributoConversionFlags
from amarcord.db.attributi import datetime_to_attributo_int
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.attributi_map import JsonAttributiMap
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_name_and_role import AttributoIdAndRole
from amarcord.db.attributo_type import AttributoTypeBoolean
from amarcord.db.attributo_type import AttributoTypeChemical
from amarcord.db.attributo_type import AttributoTypeDateTime
from amarcord.db.attributo_type import AttributoTypeDecimal
from amarcord.db.attributo_type import AttributoTypeInt
from amarcord.db.attributo_type import AttributoTypeList
from amarcord.db.attributo_type import AttributoTypeString
from amarcord.db.chemical_type import ChemicalType
from amarcord.db.db_merge_result import DBMergeResultInput
from amarcord.db.db_merge_result import DBMergeRuntimeStatusDone
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.excel_export import create_workbook
from amarcord.db.indexing_result import DBIndexingResultInput
from amarcord.db.indexing_result import DBIndexingResultStatistic
from amarcord.db.merge_model import MergeModel
from amarcord.db.merge_parameters import DBMergeParameters
from amarcord.db.merge_result import MergeResult
from amarcord.db.merge_result import MergeResultFom
from amarcord.db.merge_result import MergeResultOuterShell
from amarcord.db.merge_result import MergeResultShell
from amarcord.db.run_external_id import RunExternalId
from amarcord.db.run_internal_id import RunInternalId
from amarcord.db.scale_intensities import ScaleIntensities
from amarcord.db.schedule_entry import BeamtimeScheduleEntry
from amarcord.db.table_classes import BeamtimeInput
from amarcord.db.tables import create_tables_from_metadata
from amarcord.db.user_configuration import UserConfiguration

_EVENT_SOURCE: Final = "P11User"
_TEST_DB_URL: Final = "sqlite+aiosqlite://"
_TEST_ATTRIBUTO_VALUE: Final = 3
_TEST_EXPERIMENT_TYPE_NAME = "name-based"
_TEST_SECOND_ATTRIBUTO_VALUE: Final = 4
_TEST_CHEMICAL_NAME: Final = "chemicalname"
_TEST_CHEMICAL_RESPONSIBLE_PERSON: Final = "Rosalind Franklin"
_TEST_RUN_ID: Final = RunExternalId(1)
_TEST_ATTRIBUTO_GROUP: Final = "testgroup"
_TEST_ATTRIBUTO_DESCRIPTION: Final = "testdescription"
_TEST_ATTRIBUTO_NAME: Final = "testname"
_TEST_SECOND_ATTRIBUTO_NAME: Final = "testname1"
_TEST_BEAMTIME = BeamtimeInput(
    external_id="1101",
    proposal="prop",
    beamline="bl",
    title="title",
    comment="comment",
    start=datetime.datetime.utcnow(),
    end=datetime.datetime.utcnow() + datetime.timedelta(days=1),
)


async def _get_db(use_sqlalchemy_default_json_serializer: bool = False) -> AsyncDB:
    context = AsyncDBContext(
        _TEST_DB_URL,
        use_sqlalchemy_default_json_serializer=use_sqlalchemy_default_json_serializer,
    )
    db = AsyncDB(context, create_tables_from_metadata(context.metadata))
    await db.migrate()
    return db


async def test_create_and_retrieve_single_beamtime() -> None:
    """Create a beamtime, and retrieve it again"""
    db = await _get_db()

    async with db.begin() as conn:
        # Now create one!
        beamtime_id = await db.create_beamtime(conn, _TEST_BEAMTIME)
        assert (
            await db.retrieve_beamtime(conn, beamtime_id)
        ).external_id == _TEST_BEAMTIME.external_id


async def test_create_and_update_single_beamtime() -> None:
    """Create a beamtime, update it, and retrieve it again"""
    db = await _get_db()

    async with db.begin() as conn:
        # Now create one!
        beamtime_id = await db.create_beamtime(conn, _TEST_BEAMTIME)
        await db.update_beamtime(
            conn,
            beamtime_id=beamtime_id,
            external_id=_TEST_BEAMTIME.external_id + "new",
            proposal=_TEST_BEAMTIME.proposal + "new",
            beamline=_TEST_BEAMTIME.beamline + "new",
            title=_TEST_BEAMTIME.title + "new",
            comment=_TEST_BEAMTIME.comment + "new",
            start=_TEST_BEAMTIME.start + datetime.timedelta(days=1),
            end=_TEST_BEAMTIME.end + datetime.timedelta(days=1),
        )
        updated_beamtime = await db.retrieve_beamtime(conn, beamtime_id)
        assert updated_beamtime.external_id == _TEST_BEAMTIME.external_id + "new"
        assert updated_beamtime.proposal == _TEST_BEAMTIME.proposal + "new"
        assert updated_beamtime.beamline == _TEST_BEAMTIME.beamline + "new"
        assert updated_beamtime.title == _TEST_BEAMTIME.title + "new"
        assert updated_beamtime.comment == _TEST_BEAMTIME.comment + "new"
        assert updated_beamtime.start == _TEST_BEAMTIME.start + datetime.timedelta(
            days=1
        )
        assert updated_beamtime.end == _TEST_BEAMTIME.end + datetime.timedelta(days=1)


async def test_create_and_retrieve_beamtimes_with_chemicals() -> None:
    """Create a beamtime, and a chemical in it, and try to retrieve that"""
    db = await _get_db()

    async with db.begin() as conn:
        # Quick check beforehand: what about a completely empty DB?
        beamtimes = await db.retrieve_beamtimes(conn)

        assert not beamtimes

        # Now create one!
        beamtime_id = await db.create_beamtime(conn, _TEST_BEAMTIME)

        await db.create_chemical(
            conn=conn,
            name=_TEST_CHEMICAL_NAME,
            beamtime_id=beamtime_id,
            responsible_person=_TEST_CHEMICAL_RESPONSIBLE_PERSON,
            type_=ChemicalType.CRYSTAL,
            attributi=AttributiMap.from_types_and_json_dict(
                [],
                json_dict={},
            ),
        )
        await db.create_chemical(
            conn=conn,
            name=_TEST_CHEMICAL_NAME + "second",
            beamtime_id=beamtime_id,
            responsible_person=_TEST_CHEMICAL_RESPONSIBLE_PERSON,
            type_=ChemicalType.CRYSTAL,
            attributi=AttributiMap.from_types_and_json_dict(
                [],
                json_dict={},
            ),
        )

        beamtimes = await db.retrieve_beamtimes(conn)

        assert len(beamtimes) == 1
        assert beamtimes[0].external_id == _TEST_BEAMTIME.external_id
        assert beamtimes[0].chemical_names is not None
        assert set(beamtimes[0].chemical_names) == {
            _TEST_CHEMICAL_NAME,
            _TEST_CHEMICAL_NAME + "second",
        }


async def test_create_and_retrieve_attributo() -> None:
    """Just a really simple test: create a single attributo for a chemical, then retrieve it"""
    db = await _get_db()

    async with db.begin() as conn:
        beamtime_id = await db.create_beamtime(conn, _TEST_BEAMTIME)

        await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            beamtime_id,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.CHEMICAL,
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(
            conn, beamtime_id, AssociatedTable.CHEMICAL
        )
        assert len(attributi) == 1
        assert attributi[0].attributo_type == AttributoTypeInt()
        assert attributi[0].name == _TEST_ATTRIBUTO_NAME
        assert attributi[0].description == _TEST_ATTRIBUTO_DESCRIPTION
        assert attributi[0].group == _TEST_ATTRIBUTO_GROUP

        # Check if the filter for the table works: there should be no attributi for runs
        assert not [
            a
            for a in await db.retrieve_attributi(conn, beamtime_id, AssociatedTable.RUN)
            if a.name == _TEST_ATTRIBUTO_NAME
        ]


async def test_create_and_delete_unused_attributo() -> None:
    """Create an attributo, then delete it again"""

    db = await _get_db()

    async with db.begin() as conn:
        beamtime_id = await db.create_beamtime(conn, _TEST_BEAMTIME)

        created_attributo_id = await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            beamtime_id,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.CHEMICAL,
            AttributoTypeInt(),
        )

        await db.delete_attributo(conn, created_attributo_id)

        assert not [
            a
            for a in await db.retrieve_attributi(
                conn, beamtime_id, associated_table=None
            )
            if a.name == _TEST_ATTRIBUTO_NAME
        ]


async def test_create_and_retrieve_chemical_with_nan() -> None:
    """Create an attributo, then a chemical, and then retrieve that. NaN value is used"""

    db = await _get_db()

    async with db.begin() as conn:
        beamtime_id = await db.create_beamtime(conn, _TEST_BEAMTIME)

        test_attributo_id = await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            beamtime_id,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.CHEMICAL,
            AttributoTypeDecimal(),
        )

        attributi = await db.retrieve_attributi(
            conn, beamtime_id, associated_table=None
        )

        with pytest.raises(Exception):
            await db.create_chemical(
                conn=conn,
                name=_TEST_CHEMICAL_NAME,
                beamtime_id=beamtime_id,
                responsible_person=_TEST_CHEMICAL_RESPONSIBLE_PERSON,
                type_=ChemicalType.CRYSTAL,
                attributi=AttributiMap.from_types_and_json_dict(
                    attributi,
                    json_dict={str(test_attributo_id): numpy.NAN},
                ),
            )


async def test_create_and_retrieve_chemical() -> None:
    """Create an attributo, then a chemical, and then retrieve that"""

    db = await _get_db()

    async with db.begin() as conn:
        beamtime_id = await db.create_beamtime(conn, _TEST_BEAMTIME)

        test_attributo_id = await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            beamtime_id,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.CHEMICAL,
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(
            conn, beamtime_id, associated_table=None
        )

        chemical_id = await db.create_chemical(
            conn=conn,
            name=_TEST_CHEMICAL_NAME,
            beamtime_id=beamtime_id,
            responsible_person=_TEST_CHEMICAL_RESPONSIBLE_PERSON,
            type_=ChemicalType.CRYSTAL,
            attributi=AttributiMap.from_types_and_json_dict(
                attributi,
                json_dict={str(test_attributo_id): _TEST_ATTRIBUTO_VALUE},
            ),
        )

        chemicals = await db.retrieve_chemicals(conn, beamtime_id, attributi)

        assert len(chemicals) == 1
        assert chemicals[0].name == _TEST_CHEMICAL_NAME
        assert chemicals[0].id == chemical_id
        assert not chemicals[0].files
        assert (
            chemicals[0].attributi.select_int_unsafe(test_attributo_id)
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
        beamtime_id = await db.create_beamtime(conn, _TEST_BEAMTIME)

        first_attributo_id = await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            beamtime_id,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.CHEMICAL,
            AttributoTypeInt(),
        )
        second_attributo_id = await db.create_attributo(
            conn,
            _TEST_SECOND_ATTRIBUTO_NAME,
            beamtime_id,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.CHEMICAL,
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(
            conn, beamtime_id, associated_table=None
        )

        chemical_id = await db.create_chemical(
            conn=conn,
            name=_TEST_CHEMICAL_NAME,
            beamtime_id=beamtime_id,
            type_=ChemicalType.CRYSTAL,
            responsible_person=_TEST_CHEMICAL_RESPONSIBLE_PERSON,
            attributi=AttributiMap.from_types_and_json_dict(
                attributi,
                json_dict={
                    str(first_attributo_id): _TEST_ATTRIBUTO_VALUE,
                    str(second_attributo_id): _TEST_SECOND_ATTRIBUTO_VALUE,
                },
            ),
        )

        await db.update_chemical(
            conn=conn,
            id_=chemical_id,
            name=_TEST_CHEMICAL_NAME + "1",
            type_=ChemicalType.CRYSTAL,
            responsible_person=_TEST_CHEMICAL_RESPONSIBLE_PERSON,
            attributi=AttributiMap.from_types_and_raw(
                attributi,
                raw_attributi={
                    first_attributo_id: _TEST_ATTRIBUTO_VALUE + 1,
                    second_attributo_id: _TEST_SECOND_ATTRIBUTO_VALUE,
                },
            ),
        )

        chemicals = await db.retrieve_chemicals(conn, beamtime_id, attributi)

        assert len(chemicals) == 1
        assert chemicals[0].name == _TEST_CHEMICAL_NAME + "1"
        assert chemicals[0].id == chemical_id
        assert (
            chemicals[0].attributi.select_int_unsafe(first_attributo_id)
            == _TEST_ATTRIBUTO_VALUE + 1
        )
        assert (
            chemicals[0].attributi.select_int_unsafe(second_attributo_id)
            == _TEST_SECOND_ATTRIBUTO_VALUE
        )


async def test_create_and_delete_chemical() -> None:
    """Create an attributo, then a chemical, delete the chemical again"""

    db = await _get_db()

    async with db.begin() as conn:
        beamtime_id = await db.create_beamtime(conn, _TEST_BEAMTIME)

        first_attributo_id = await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            beamtime_id,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.CHEMICAL,
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(
            conn, beamtime_id, associated_table=None
        )

        chemical_id = await db.create_chemical(
            conn=conn,
            name=_TEST_CHEMICAL_NAME,
            beamtime_id=beamtime_id,
            responsible_person=_TEST_CHEMICAL_RESPONSIBLE_PERSON,
            type_=ChemicalType.CRYSTAL,
            attributi=AttributiMap.from_types_and_json_dict(
                attributi,
                json_dict={str(first_attributo_id): _TEST_ATTRIBUTO_VALUE},
            ),
        )

        await db.delete_chemical(conn, chemical_id, delete_in_dependencies=True)
        assert not await db.retrieve_chemicals(conn, beamtime_id, attributi)


async def test_create_attributo_and_chemical_then_change_attributo() -> None:
    """Create an attributo, then a chemical, then change the attributo. Should propagate to the chemical"""

    db = await _get_db()

    async with db.begin() as conn:
        beamtime_id = await db.create_beamtime(conn, _TEST_BEAMTIME)

        test_attributo_id = await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            beamtime_id,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.CHEMICAL,
            # We create the attributo as int, then convert to string
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(
            conn, beamtime_id, associated_table=None
        )

        await db.create_chemical(
            conn=conn,
            name=_TEST_CHEMICAL_NAME,
            beamtime_id=beamtime_id,
            type_=ChemicalType.CRYSTAL,
            responsible_person=_TEST_CHEMICAL_RESPONSIBLE_PERSON,
            attributi=AttributiMap.from_types_and_json_dict(
                attributi,
                json_dict={str(test_attributo_id): _TEST_ATTRIBUTO_VALUE},
            ),
        )

        await db.update_attributo(
            conn,
            id_=test_attributo_id,
            beamtime_id=beamtime_id,
            conversion_flags=AttributoConversionFlags(ignore_units=False),
            new_attributo=DBAttributo(
                id=test_attributo_id,
                beamtime_id=beamtime_id,
                # We try to change all the attributes
                name=str(_TEST_ATTRIBUTO_NAME) + "1",
                description=_TEST_ATTRIBUTO_DESCRIPTION + "1",
                group=_TEST_ATTRIBUTO_GROUP + "1",
                associated_table=AssociatedTable.CHEMICAL,
                attributo_type=AttributoTypeString(),
            ),
        )

        attributi = await db.retrieve_attributi(
            conn, beamtime_id, associated_table=None
        )
        test_attributi = [a for a in attributi if a.name == _TEST_ATTRIBUTO_NAME + "1"]

        assert len(test_attributi) == 1
        assert test_attributi[0].name == _TEST_ATTRIBUTO_NAME + "1"
        assert test_attributi[0].description == _TEST_ATTRIBUTO_DESCRIPTION + "1"
        assert test_attributi[0].group == _TEST_ATTRIBUTO_GROUP + "1"

        chemicals = await db.retrieve_chemicals(conn, beamtime_id, attributi)
        with pytest.raises(AssertionError):
            # Getting an int shouldn't work
            assert chemicals[0].attributi.select_int(test_attributo_id) is None

        assert chemicals[0].attributi.select_string(test_attributo_id) is not None


async def test_create_attributo_and_chemical_then_delete_attributo() -> None:
    """Create an attributo, then a chemical, then delete the attributo. Should propagate to the chemical"""

    db = await _get_db()

    async with db.begin() as conn:
        beamtime_id = await db.create_beamtime(conn, _TEST_BEAMTIME)

        test_attributo_id = await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            beamtime_id,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.CHEMICAL,
            # We create the attributo as int, then convert to string
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(
            conn, beamtime_id, associated_table=None
        )

        await db.create_chemical(
            conn=conn,
            beamtime_id=beamtime_id,
            name=_TEST_CHEMICAL_NAME,
            type_=ChemicalType.CRYSTAL,
            responsible_person=_TEST_CHEMICAL_RESPONSIBLE_PERSON,
            attributi=AttributiMap.from_types_and_json_dict(
                attributi,
                json_dict={str(test_attributo_id): _TEST_ATTRIBUTO_VALUE},
            ),
        )

        await db.delete_attributo(conn, test_attributo_id)

        attributi = await db.retrieve_attributi(
            conn, beamtime_id, associated_table=None
        )

        chemicals = await db.retrieve_chemicals(conn, beamtime_id, attributi)
        assert chemicals[0].attributi.select_int(test_attributo_id) is None


async def test_create_and_retrieve_runs() -> None:
    """Create (non-chemical) attributi, then a run, and then retrieve that"""

    db = await _get_db()

    async with db.begin() as conn:
        beamtime_id = await db.create_beamtime(conn, _TEST_BEAMTIME)

        test_int_attributo_id = await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            beamtime_id,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            AttributoTypeInt(),
        )

        test_decimal_attributo_id = await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME + "decimal",
            beamtime_id,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            AttributoTypeDecimal(),
        )

        test_list_attributo_id = await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME + "decimal",
            beamtime_id,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            AttributoTypeList(
                sub_type=AttributoTypeString(), min_length=None, max_length=None
            ),
        )

        test_bool_attributo_id = await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME + "bool",
            beamtime_id,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            AttributoTypeBoolean(),
        )

        test_datetime_attributo_id = await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME + "datetime",
            beamtime_id,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            AttributoTypeDateTime(),
        )

        attributi = await db.retrieve_attributi(
            conn, beamtime_id, associated_table=None
        )

        experiment_type_id = await db.create_experiment_type(
            conn,
            name=_TEST_EXPERIMENT_TYPE_NAME,
            beamtime_id=beamtime_id,
            experiment_attributi=[
                AttributoIdAndRole(test_int_attributo_id, ChemicalType.CRYSTAL)
            ],
        )

        test_decimal_value = 3.5
        datetime_original = datetime.datetime.utcnow()
        test_datetime_value = datetime_to_attributo_int(datetime_original)
        test_bool_value = True
        test_list_value = ["a", "b"]
        run_internal_id = await db.create_run(
            conn,
            beamtime_id=beamtime_id,
            run_external_id=_TEST_RUN_ID,
            started=datetime.datetime.utcnow(),
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_json_dict(
                attributi,
                json_dict={
                    str(test_int_attributo_id): _TEST_ATTRIBUTO_VALUE,
                    str(test_decimal_attributo_id): test_decimal_value,
                    str(test_datetime_attributo_id): test_datetime_value,
                    str(test_bool_attributo_id): test_bool_value,
                    str(test_list_attributo_id): test_list_value,
                },
            ),
            experiment_type_id=experiment_type_id,
            keep_manual_attributes_from_previous_run=False,
        )

        runs = await db.retrieve_runs(conn, beamtime_id, attributi)

        assert len(runs) == 1
        assert runs[0].id == run_internal_id
        assert runs[0].external_id == _TEST_RUN_ID
        assert not runs[0].files
        assert (
            runs[0].attributi.select_int_unsafe(test_int_attributo_id)
            == _TEST_ATTRIBUTO_VALUE
        )
        assert (
            runs[0].attributi.select_unsafe(test_list_attributo_id) == test_list_value
        )
        assert (
            runs[0].attributi.select_bool_unsafe(test_bool_attributo_id)
            == test_bool_value
        )
        # The comparison is a little tricky here, because we are not storing datetimes with their maximum (nanosecond?) precision in the DB, so we have to convert whatever we got from the DB into milliseconds again.
        assert (
            datetime_to_attributo_int(
                runs[0].attributi.select_datetime_unsafe(test_datetime_attributo_id)
            )
            == test_datetime_value
        )
        assert pytest.approx(
            test_decimal_value,
            runs[0].attributi.select_float_unsafe(test_decimal_attributo_id),
        )


async def test_create_and_retrieve_runs_and_one_chemical_attached() -> None:
    """Create a chemical attributo, then a run and a chemical, and then retrieve that"""

    db = await _get_db()

    async with db.begin() as conn:
        beamtime_id = await db.create_beamtime(conn, _TEST_BEAMTIME)

        test_chemical_attributo_id = await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            beamtime_id,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            AttributoTypeChemical(),
        )

        attributi = await db.retrieve_attributi(
            conn, beamtime_id, associated_table=None
        )

        experiment_type_id = await db.create_experiment_type(
            conn,
            name=_TEST_EXPERIMENT_TYPE_NAME,
            beamtime_id=beamtime_id,
            experiment_attributi=[
                AttributoIdAndRole(test_chemical_attributo_id, ChemicalType.CRYSTAL)
            ],
        )

        chemical_id = await db.create_chemical(
            conn=conn,
            name=_TEST_CHEMICAL_NAME,
            beamtime_id=beamtime_id,
            responsible_person=_TEST_CHEMICAL_RESPONSIBLE_PERSON,
            type_=ChemicalType.CRYSTAL,
            attributi=AttributiMap.from_types_and_json_dict(
                attributi,
                json_dict={},
            ),
        )

        run_internal_id = await db.create_run(
            conn,
            beamtime_id=beamtime_id,
            started=datetime.datetime.utcnow(),
            run_external_id=_TEST_RUN_ID,
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_json_dict(
                attributi,
                json_dict={
                    str(test_chemical_attributo_id): chemical_id,
                },
            ),
            experiment_type_id=experiment_type_id,
            keep_manual_attributes_from_previous_run=False,
        )

        runs = await db.retrieve_runs(conn, beamtime_id, attributi)

        assert len(runs) == 1
        assert runs[0].id == run_internal_id
        assert (
            runs[0].attributi.select_chemical_id(test_chemical_attributo_id)
            == chemical_id
        )


async def test_create_and_retrieve_run() -> None:
    """Create an attributo, then a run, and then retrieve just that run"""

    db = await _get_db()

    async with db.begin() as conn:
        beamtime_id = await db.create_beamtime(conn, _TEST_BEAMTIME)

        test_attributo_id = await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            beamtime_id,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(
            conn, beamtime_id, associated_table=None
        )
        experiment_type_id = await db.create_experiment_type(
            conn,
            beamtime_id=beamtime_id,
            name=_TEST_EXPERIMENT_TYPE_NAME,
            experiment_attributi=[
                AttributoIdAndRole(test_attributo_id, ChemicalType.CRYSTAL)
            ],
        )

        run_internal_id = await db.create_run(
            conn,
            beamtime_id=beamtime_id,
            started=datetime.datetime.utcnow(),
            run_external_id=_TEST_RUN_ID,
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_json_dict(
                attributi,
                json_dict={str(test_attributo_id): _TEST_ATTRIBUTO_VALUE},
            ),
            experiment_type_id=experiment_type_id,
            keep_manual_attributes_from_previous_run=False,
        )

        run = await db.retrieve_run(
            conn,
            internal_id=run_internal_id,
            attributi=attributi,
        )

        assert run is not None
        assert run.id == run_internal_id
        assert run.external_id == _TEST_RUN_ID
        assert not run.files
        assert (
            run.attributi.select_int_unsafe(test_attributo_id) == _TEST_ATTRIBUTO_VALUE
        )

        run = await db.retrieve_latest_run(conn, beamtime_id, attributi)

        assert run is not None
        assert run.id == _TEST_RUN_ID
        assert not run.files
        assert (
            run.attributi.select_int_unsafe(test_attributo_id) == _TEST_ATTRIBUTO_VALUE
        )


async def test_create_run_and_then_next_run_using_previous_attributi() -> None:
    """Create a run with some attributi, then another run and test the "keep attributi from previous" feature"""

    db = await _get_db()

    async with db.begin() as conn:
        beamtime_id = await db.create_beamtime(conn, _TEST_BEAMTIME)

        # The mechanism to copy over attributes from the previous run is hard-coded to "manual" attributi, so let's
        # create one that's manual and one that isn't
        first_attributo_id = await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            beamtime_id,
            _TEST_ATTRIBUTO_DESCRIPTION,
            ATTRIBUTO_GROUP_MANUAL,
            AssociatedTable.RUN,
            AttributoTypeInt(),
        )
        second_test_attribute = str(_TEST_ATTRIBUTO_NAME) + "2"
        second_attributo_id = await db.create_attributo(
            conn,
            second_test_attribute,
            beamtime_id,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(
            conn, beamtime_id, associated_table=None
        )
        experiment_type_id = await db.create_experiment_type(
            conn,
            name=_TEST_EXPERIMENT_TYPE_NAME,
            beamtime_id=beamtime_id,
            experiment_attributi=[
                AttributoIdAndRole(first_attributo_id, ChemicalType.CRYSTAL)
            ],
        )

        await db.create_run(
            conn,
            beamtime_id=beamtime_id,
            started=datetime.datetime.utcnow(),
            run_external_id=_TEST_RUN_ID,
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_json_dict(
                attributi,
                # Only one of the two attributi here (the manual one)
                json_dict={str(first_attributo_id): _TEST_ATTRIBUTO_VALUE},
            ),
            experiment_type_id=experiment_type_id,
            # Flag doesn't matter if it's just one run
            keep_manual_attributes_from_previous_run=False,
        )

        attributi = await db.retrieve_attributi(
            conn, beamtime_id, associated_table=None
        )

        second_test_attribute_value = _TEST_ATTRIBUTO_VALUE + 1
        await db.create_run(
            conn,
            beamtime_id=beamtime_id,
            started=datetime.datetime.utcnow(),
            # Next Run ID
            run_external_id=RunExternalId(_TEST_RUN_ID + 1),
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_json_dict(
                attributi,
                # The other attributo with a different value
                json_dict={str(second_attributo_id): second_test_attribute_value},
            ),
            experiment_type_id=experiment_type_id,
            # Keep previous (manual) attributi
            keep_manual_attributes_from_previous_run=True,
        )

        runs = await db.retrieve_runs(conn, beamtime_id, attributi)
        assert len(runs) == 2
        # Assume ordering by ID descending
        assert runs[0].external_id == _TEST_RUN_ID + 1
        assert runs[0].attributi.select_int(first_attributo_id) == _TEST_ATTRIBUTO_VALUE
        assert (
            runs[0].attributi.select_int(second_attributo_id)
            == second_test_attribute_value
        )


async def test_create_attributo_and_run_then_change_attributo() -> None:
    """Create an attributo, then a run, then change the attributo. Should propagate to the run"""

    db = await _get_db()

    async with db.begin() as conn:
        beamtime_id = await db.create_beamtime(conn, _TEST_BEAMTIME)

        first_attributo_id = await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            beamtime_id,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            # We create the attributo as int, then convert to string
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(
            conn, beamtime_id, associated_table=None
        )
        experiment_type_id = await db.create_experiment_type(
            conn,
            name=_TEST_EXPERIMENT_TYPE_NAME,
            beamtime_id=beamtime_id,
            experiment_attributi=[
                AttributoIdAndRole(first_attributo_id, ChemicalType.CRYSTAL)
            ],
        )

        await db.create_run(
            conn,
            run_external_id=_TEST_RUN_ID,
            started=datetime.datetime.utcnow(),
            beamtime_id=beamtime_id,
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_json_dict(
                attributi,
                json_dict={str(first_attributo_id): _TEST_ATTRIBUTO_VALUE},
            ),
            experiment_type_id=experiment_type_id,
            keep_manual_attributes_from_previous_run=False,
        )

        await db.update_attributo(
            conn,
            id_=first_attributo_id,
            beamtime_id=beamtime_id,
            conversion_flags=AttributoConversionFlags(ignore_units=False),
            new_attributo=DBAttributo(
                id=first_attributo_id,
                beamtime_id=beamtime_id,
                # We try to change all the attributes
                name=str(_TEST_ATTRIBUTO_NAME) + "1",
                description=_TEST_ATTRIBUTO_DESCRIPTION + "1",
                group=_TEST_ATTRIBUTO_GROUP + "1",
                associated_table=AssociatedTable.RUN,
                attributo_type=AttributoTypeString(),
            ),
        )

        attributi = await db.retrieve_attributi(
            conn, beamtime_id, associated_table=None
        )

        runs = await db.retrieve_runs(conn, beamtime_id, attributi)

        # Getting an int shouldn't work anymore
        with pytest.raises(AssertionError):
            assert runs[0].attributi.select_int(first_attributo_id) is None

        assert runs[0].attributi.select_string(first_attributo_id) is not None


async def test_create_attributo_and_run_then_delete_attributo() -> None:
    """Create an attributo, then a run, then change the attributo. Should propagate to the run"""

    db = await _get_db()

    async with db.begin() as conn:
        beamtime_id = await db.create_beamtime(conn, _TEST_BEAMTIME)

        attributo_id = await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            beamtime_id,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            # We create the attributo as int, then convert to string
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(
            conn, beamtime_id, associated_table=None
        )
        experiment_type_id = await db.create_experiment_type(
            conn,
            name=_TEST_EXPERIMENT_TYPE_NAME,
            beamtime_id=beamtime_id,
            experiment_attributi=[
                AttributoIdAndRole(attributo_id, ChemicalType.CRYSTAL)
            ],
        )

        await db.create_run(
            conn,
            beamtime_id=beamtime_id,
            started=datetime.datetime.utcnow(),
            run_external_id=_TEST_RUN_ID,
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_raw(
                attributi,
                raw_attributi={attributo_id: _TEST_ATTRIBUTO_VALUE},
            ),
            experiment_type_id=experiment_type_id,
            keep_manual_attributes_from_previous_run=False,
        )

        await db.delete_attributo(conn, attributo_id)

        attributi = await db.retrieve_attributi(
            conn, beamtime_id, associated_table=None
        )

        runs = await db.retrieve_runs(conn, beamtime_id, attributi)
        assert runs[0].attributi.select_int(attributo_id) is None


async def test_create_attributo_and_run_and_chemical_for_run_then_delete_chemical() -> None:
    """This is a bit of an edge case: we have an attributo that signifies the chemical of a run, and we create a run and a chemical, and then we delete the chemical"""

    db = await _get_db()

    async with db.begin() as conn:
        beamtime_id = await db.create_beamtime(conn, _TEST_BEAMTIME)

        attributo_id = await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            beamtime_id,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            AttributoTypeChemical(),
        )

        attributi = await db.retrieve_attributi(
            conn, beamtime_id, associated_table=None
        )

        chemical_id = await db.create_chemical(
            conn=conn,
            name=_TEST_CHEMICAL_NAME,
            beamtime_id=beamtime_id,
            responsible_person=_TEST_CHEMICAL_RESPONSIBLE_PERSON,
            type_=ChemicalType.CRYSTAL,
            attributi=AttributiMap.from_types_and_json_dict(attributi, json_dict={}),
        )

        # Create an experiment type and a data-set. This is supposed to be deleted as well when we delete the chemical
        # attributo.
        et_id = await db.create_experiment_type(
            conn,
            name="chemical-based",
            beamtime_id=beamtime_id,
            experiment_attributi=[
                AttributoIdAndRole(attributo_id, ChemicalType.CRYSTAL)
            ],
        )

        ds_id = await db.create_data_set(
            conn,
            beamtime_id,
            et_id,
            AttributiMap.from_types_and_raw(attributi, {attributo_id: chemical_id}),
        )

        retrieved_data_set = await db.retrieve_data_set(
            conn, data_set_id=ds_id, attributi=attributi
        )
        assert retrieved_data_set is not None

        await db.create_run(
            conn,
            beamtime_id=beamtime_id,
            started=datetime.datetime.utcnow(),
            run_external_id=_TEST_RUN_ID,
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_raw(
                attributi,
                raw_attributi={attributo_id: chemical_id},
            ),
            experiment_type_id=et_id,
            keep_manual_attributes_from_previous_run=False,
        )

        with pytest.raises(Exception):
            # This doesn't work, because the chemical is being used
            await db.delete_chemical(conn, chemical_id, delete_in_dependencies=False)

        # This works, because we explicitly say we want to delete it from the runs
        await db.delete_chemical(conn, chemical_id, delete_in_dependencies=True)

        runs = await db.retrieve_runs(conn, beamtime_id, attributi)
        assert runs[0].attributi.select_chemical_id(attributo_id) is None

        assert not await db.retrieve_data_sets(conn, beamtime_id, attributi)


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

        file_ = await db.retrieve_file(conn, result.id, with_contents=False)

        assert file_.file_name == "name.txt"
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
        beamtime_id = await db.create_beamtime(conn, _TEST_BEAMTIME)

        first_name = "a1"
        first_id = await db.create_attributo(
            conn,
            beamtime_id=beamtime_id,
            name=str(first_name),
            description="",
            group=ATTRIBUTO_GROUP_MANUAL,
            associated_table=AssociatedTable.RUN,
            type_=AttributoTypeInt(),
        )
        second_name = "a2"
        second_id = await db.create_attributo(
            conn,
            beamtime_id=beamtime_id,
            name=str(second_name),
            description="",
            group=ATTRIBUTO_GROUP_MANUAL,
            associated_table=AssociatedTable.RUN,
            type_=AttributoTypeString(),
        )

        # First try with a nonexistent attributo name
        e_type_name = "e1"
        with pytest.raises(Exception):
            await db.create_experiment_type(
                conn,
                beamtime_id,
                e_type_name,
                [
                    AttributoIdAndRole(first_id, ChemicalType.CRYSTAL),
                    AttributoIdAndRole(
                        AttributoId(first_id * 1000), ChemicalType.CRYSTAL
                    ),
                ],
            )

        # Create the experiment type, then retrieve it, then delete it again and check if that worked
        et_id = await db.create_experiment_type(
            conn,
            beamtime_id,
            e_type_name,
            [
                AttributoIdAndRole(first_id, ChemicalType.CRYSTAL),
                AttributoIdAndRole(second_id, ChemicalType.SOLUTION),
            ],
        )

        e_types = list(await db.retrieve_experiment_types(conn, beamtime_id))
        assert len(e_types) == 1
        assert e_types[0].name == e_type_name
        assert e_types[0].attributi == [
            AttributoIdAndRole(first_id, ChemicalType.CRYSTAL),
            AttributoIdAndRole(second_id, ChemicalType.SOLUTION),
        ]

        # Now delete it again
        await db.delete_experiment_type(conn, et_id)

        assert not await db.retrieve_experiment_types(conn, beamtime_id)


async def test_create_and_retrieve_data_sets() -> None:
    db = await _get_db()

    async with db.begin() as conn:
        beamtime_id = await db.create_beamtime(conn, _TEST_BEAMTIME)

        first_name = "a1"
        first_id = await db.create_attributo(
            conn,
            beamtime_id=beamtime_id,
            name=str(first_name),
            description="",
            group=ATTRIBUTO_GROUP_MANUAL,
            associated_table=AssociatedTable.RUN,
            type_=AttributoTypeInt(),
        )
        second_name = "a2"
        second_id = await db.create_attributo(
            conn,
            beamtime_id=beamtime_id,
            name=str(second_name),
            description="",
            group=ATTRIBUTO_GROUP_MANUAL,
            associated_table=AssociatedTable.RUN,
            type_=AttributoTypeString(),
        )

        # Create experiment type
        e_type_name = "e1"
        et_id = await db.create_experiment_type(
            conn,
            beamtime_id,
            e_type_name,
            [
                AttributoIdAndRole(first_id, ChemicalType.CRYSTAL),
                AttributoIdAndRole(second_id, ChemicalType.CRYSTAL),
            ],
        )

        attributi = await db.retrieve_attributi(
            conn, beamtime_id, associated_table=None
        )

        raw_attributi = {str(first_id): 1, str(second_id): "f"}
        id_ = await db.create_data_set(
            conn,
            beamtime_id,
            et_id,
            AttributiMap.from_types_and_json_dict(
                types=attributi,
                json_dict=raw_attributi,
            ),
        )

        assert id_ > 0

        data_sets = await db.retrieve_data_sets(
            conn,
            beamtime_id,
            attributi,
        )

        assert len(data_sets) == 1
        assert data_sets[0].id == id_
        assert data_sets[0].experiment_type_id == et_id
        assert data_sets[0].attributi.to_json() == raw_attributi

        await db.delete_data_set(conn, id_)
        assert not (
            await db.retrieve_data_sets(
                conn,
                beamtime_id,
                attributi,
            )
        )

        with pytest.raises(Exception):
            await db.create_data_set(
                conn,
                beamtime_id,
                et_id,
                AttributiMap.from_types_and_json_dict(attributi, {}),
            )


async def test_create_data_set_and_and_change_attributo_type() -> None:
    db = await _get_db()

    async with db.begin() as conn:
        beamtime_id = await db.create_beamtime(conn, _TEST_BEAMTIME)

        first_name = "a1"
        first_id = await db.create_attributo(
            conn,
            beamtime_id=beamtime_id,
            name=str(first_name),
            description="",
            group=ATTRIBUTO_GROUP_MANUAL,
            associated_table=AssociatedTable.RUN,
            type_=AttributoTypeInt(),
        )
        second_name = "a2"
        second_id = await db.create_attributo(
            conn,
            beamtime_id=beamtime_id,
            name=str(second_name),
            description="",
            group=ATTRIBUTO_GROUP_MANUAL,
            associated_table=AssociatedTable.RUN,
            type_=AttributoTypeString(),
        )

        # Create experiment type
        e_type_name = "e1"
        et_id = await db.create_experiment_type(
            conn,
            beamtime_id,
            e_type_name,
            [
                AttributoIdAndRole(first_id, ChemicalType.CRYSTAL),
                AttributoIdAndRole(second_id, ChemicalType.CRYSTAL),
            ],
        )

        attributi = await db.retrieve_attributi(
            conn, beamtime_id, associated_table=None
        )

        raw_attributi = {str(first_id): 1, str(second_id): "f"}
        await db.create_data_set(
            conn,
            beamtime_id,
            et_id,
            AttributiMap.from_types_and_json_dict(
                types=attributi,
                json_dict=raw_attributi,
            ),
        )

        await db.update_attributo(
            conn,
            first_id,
            beamtime_id,
            AttributoConversionFlags(ignore_units=False),
            new_attributo=DBAttributo(
                id=first_id,
                beamtime_id=beamtime_id,
                name=first_name,
                description="",
                group=ATTRIBUTO_GROUP_MANUAL,
                associated_table=AssociatedTable.RUN,
                attributo_type=AttributoTypeString(),
            ),
        )

        data_sets = list(
            await db.retrieve_data_sets(
                conn,
                beamtime_id,
                await db.retrieve_attributi(conn, beamtime_id, associated_table=None),
            )
        )
        assert data_sets[0].attributi.select_string(first_id)


async def test_create_read_delete_events() -> None:
    db = await _get_db()

    async with db.begin() as conn:
        beamtime_id = await db.create_beamtime(conn, _TEST_BEAMTIME)

        event_text = "hihi"
        event_level = EventLogLevel.INFO
        event_id = await db.create_event(
            conn, beamtime_id, event_level, _EVENT_SOURCE, event_text
        )

        assert event_id > 0

        events = await db.retrieve_events(conn, beamtime_id, None)

        assert len(events) == 1

        assert events[0].id == event_id
        assert events[0].source == _EVENT_SOURCE
        assert events[0].text == event_text
        assert events[0].level == event_level

        await db.create_event(conn, beamtime_id, event_level, _EVENT_SOURCE, event_text)

        await db.delete_event(conn, event_id)

        assert len(await db.retrieve_events(conn, beamtime_id, None)) == 1


async def test_create_and_retrieve_attributo_two_runs() -> None:
    db = await _get_db()

    async with db.begin() as conn:
        beamtime_id = await db.create_beamtime(conn, _TEST_BEAMTIME)

        first_id = await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            beamtime_id,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            AttributoTypeString(),
        )
        second_id = await db.create_attributo(
            conn,
            _TEST_SECOND_ATTRIBUTO_NAME,
            beamtime_id,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(
            conn, beamtime_id, associated_table=AssociatedTable.RUN
        )
        experiment_type_id = await db.create_experiment_type(
            conn,
            beamtime_id=beamtime_id,
            name=_TEST_EXPERIMENT_TYPE_NAME,
            experiment_attributi=[AttributoIdAndRole(first_id, ChemicalType.CRYSTAL)],
        )

        run_data: dict[RunExternalId, JsonAttributiMap] = {
            _TEST_RUN_ID: {
                # Important: here, we have "foo" and 1, below we have something else for "foo", but the same for 1
                str(first_id): "foo",
                str(second_id): 1,
            },
            RunExternalId(_TEST_RUN_ID + 1): {
                str(first_id): "bar",
                str(second_id): 1,
            },
        }

        run_external_to_internal_id: dict[RunExternalId, RunInternalId] = {}
        for run_external_id, data in run_data.items():
            run_external_to_internal_id[run_external_id] = await db.create_run(
                conn,
                beamtime_id=beamtime_id,
                started=datetime.datetime.utcnow(),
                run_external_id=run_external_id,
                attributi=attributi,
                attributi_map=AttributiMap.from_types_and_json_dict(
                    attributi, json_dict=data
                ),
                experiment_type_id=experiment_type_id,
                keep_manual_attributes_from_previous_run=False,
            )

        bulk_attributi = await db.retrieve_bulk_run_attributi(
            conn,
            beamtime_id=beamtime_id,
            attributi=attributi,
            run_ids=list(run_external_to_internal_id.values()),
        )

        assert bulk_attributi == {
            first_id: {"foo", "bar"},
            second_id: {1},
        }

        new_test_attributo = "baz"
        await db.update_bulk_run_attributi(
            conn,
            beamtime_id,
            attributi,
            run_ids=set(run_external_to_internal_id.values()),
            attributi_values=AttributiMap.from_types_and_raw(
                attributi,
                {
                    # We only store one new attributo for both runs and see if only that gets updated
                    first_id: new_test_attributo,
                },
            ),
            new_experiment_type_id=None,
        )

        assert (  # type: ignore
            await db.retrieve_run(
                conn, run_external_to_internal_id[_TEST_RUN_ID], attributi
            )
        ).attributi.select_string(  # pyright: ignore
            first_id
        ) == new_test_attributo

        assert (  # type: ignore
            await db.retrieve_run(
                conn,
                run_external_to_internal_id[RunExternalId(_TEST_RUN_ID + 1)],
                attributi,
            )
        ).attributi.select_string(  # pyright: ignore
            first_id
        ) == new_test_attributo


async def test_create_workbook() -> None:
    db = await _get_db()

    async with db.begin() as conn:
        beamtime_id = await db.create_beamtime(conn, _TEST_BEAMTIME)

        first_id = await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            beamtime_id,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.CHEMICAL,
            AttributoTypeString(),
        )
        second_id = await db.create_attributo(
            conn,
            _TEST_SECOND_ATTRIBUTO_NAME,
            beamtime_id,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            AttributoTypeString(),
        )

        attributi = await db.retrieve_attributi(
            conn, beamtime_id, associated_table=None
        )
        await db.create_chemical(
            conn=conn,
            beamtime_id=beamtime_id,
            name="first chemical",
            type_=ChemicalType.CRYSTAL,
            responsible_person=_TEST_CHEMICAL_RESPONSIBLE_PERSON,
            attributi=AttributiMap.from_types_and_raw(
                attributi,
                raw_attributi={first_id: "foo"},
            ),
        )
        experiment_type_id = await db.create_experiment_type(
            conn,
            beamtime_id=beamtime_id,
            name=_TEST_EXPERIMENT_TYPE_NAME,
            experiment_attributi=[AttributoIdAndRole(first_id, ChemicalType.CRYSTAL)],
        )

        await db.create_run(
            conn,
            run_external_id=RunExternalId(1),
            started=datetime.datetime.utcnow(),
            beamtime_id=beamtime_id,
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_raw(
                types=attributi,
                raw_attributi={second_id: "foo"},
            ),
            experiment_type_id=experiment_type_id,
            keep_manual_attributes_from_previous_run=False,
        )

        wb = (await create_workbook(db, conn, beamtime_id, with_events=True)).workbook

        attributi_wb = wb["Attributi"]
        assert attributi_wb is not None

        # Man am I tired of example-based testing
        assert attributi_wb["A1"].value == "Table"  # pyright: ignore
        assert attributi_wb["B1"].value == "Name"  # pyright: ignore

        runs_wb = wb["Runs"]
        assert runs_wb is not None

        assert runs_wb["A1"].value == "ID"  # pyright: ignore
        assert runs_wb["B1"].value == "started"  # pyright: ignore
        assert runs_wb["C1"].value == "stopped"  # pyright: ignore
        assert runs_wb["D1"].value == _TEST_SECOND_ATTRIBUTO_NAME  # pyright: ignore

        # A2 will have the external run ID in it
        assert runs_wb["A2"].value == 1  # pyright: ignore
        assert runs_wb["D2"].value == "foo", f"{runs_wb} wrong row"  # pyright: ignore

        chemical_wb = wb["Chemicals"]
        assert chemical_wb is not None

        assert chemical_wb["A1"].value == "Name"  # pyright: ignore
        assert chemical_wb["B1"].value == _TEST_ATTRIBUTO_NAME  # pyright: ignore

        assert chemical_wb["A2"].value == "first chemical"  # pyright: ignore
        assert chemical_wb["B2"].value == "foo"  # pyright: ignore


async def test_retrieve_and_update_configuration() -> None:
    db = await _get_db()

    async with db.begin() as conn:
        beamtime_id = await db.create_beamtime(conn, _TEST_BEAMTIME)

        assert (await db.retrieve_configuration(conn, beamtime_id)).auto_pilot
        await db.update_configuration(
            conn,
            beamtime_id,
            UserConfiguration(
                auto_pilot=False,
                use_online_crystfel=False,
                current_experiment_type_id=None,
            ),
        )
        assert not (await db.retrieve_configuration(conn, beamtime_id)).auto_pilot


async def test_create_indexing_result() -> None:
    db = await _get_db()

    async with db.begin() as conn:
        beamtime_id = await db.create_beamtime(conn, _TEST_BEAMTIME)

        attributi = await db.retrieve_attributi(
            conn, beamtime_id, associated_table=None
        )

        attributo_id = await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            beamtime_id,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.CHEMICAL,
            AttributoTypeString(),
        )
        experiment_type_id = await db.create_experiment_type(
            conn,
            beamtime_id,
            name=_TEST_EXPERIMENT_TYPE_NAME,
            experiment_attributi=[
                AttributoIdAndRole(attributo_id, ChemicalType.CRYSTAL)
            ],
        )
        run_id = await db.create_run(
            conn,
            beamtime_id=beamtime_id,
            started=datetime.datetime.utcnow(),
            run_external_id=RunExternalId(1),
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_json_dict(
                attributi, json_dict={}
            ),
            experiment_type_id=experiment_type_id,
            keep_manual_attributes_from_previous_run=False,
        )

        chemical_id = await db.create_chemical(
            conn=conn,
            beamtime_id=beamtime_id,
            name="test",
            responsible_person=_TEST_CHEMICAL_RESPONSIBLE_PERSON,
            type_=ChemicalType.CRYSTAL,
            attributi=AttributiMap.from_types_and_raw(attributi, raw_attributi={}),
        )

        indexing_result_id = await db.create_indexing_result(
            conn,
            DBIndexingResultInput(
                created=datetime.datetime.utcnow(),
                run_id=run_id,
                frames=0,
                hits=0,
                not_indexed_frames=0,
                chemical_id=chemical_id,
                cell_description=None,
                point_group=None,
                runtime_status=None,
            ),
        )

        assert indexing_result_id is not None

        await db.add_indexing_result_statistic(
            conn,
            DBIndexingResultStatistic(
                indexing_result_id=indexing_result_id,
                time=datetime.datetime.utcnow(),
                frames=1,
                hits=2,
                indexed_frames=3,
                indexed_crystals=4,
            ),
        )
        irs = await db.retrieve_indexing_result_statistics(
            conn, beamtime_id, indexing_result_id
        )
        assert len(irs) == 1
        assert irs[0].frames == 1


async def test_create_and_retrieve_beamtime_schedule() -> None:
    """Create a beamtime, and retrieve it again"""
    db = await _get_db()

    async with db.begin() as conn:
        beamtime_id = await db.create_beamtime(conn, _TEST_BEAMTIME)
        chemical_id = await db.create_chemical(
            conn=conn,
            name=_TEST_CHEMICAL_NAME,
            beamtime_id=beamtime_id,
            responsible_person=_TEST_CHEMICAL_RESPONSIBLE_PERSON,
            type_=ChemicalType.CRYSTAL,
            attributi=AttributiMap.from_types_and_json_dict(
                [],
                json_dict={},
            ),
        )

        users = "foo, bar"
        td_support = "support"
        date = "date"
        shift = "shift"
        comment = "comment"
        chemicals = [chemical_id]
        schedule = [
            BeamtimeScheduleEntry(
                users=users,
                td_support=td_support,
                date=date,
                shift=shift,
                comment=comment,
                chemicals=chemicals,
            ),
            BeamtimeScheduleEntry(
                users=users + "2",
                td_support=td_support + "2",
                date=date + "2",
                shift=shift + "2",
                comment=comment + "2",
                chemicals=[],
            ),
        ]
        await db.replace_beamtime_schedule(
            conn,
            beamtime_id,
            schedule,
        )

        new_schedule = await db.retrieve_beamtime_schedule(conn, beamtime_id)

        assert schedule == new_schedule


async def test_create_refinement_result() -> None:
    """Create a beamtime, runs, indexing result, merge result and then a refinement result (phew!)"""
    db = await _get_db()

    async with db.begin() as conn:
        beamtime_id = await db.create_beamtime(conn, _TEST_BEAMTIME)

        test_attributo_id = await db.create_attributo(
            conn,
            _TEST_ATTRIBUTO_NAME,
            beamtime_id,
            _TEST_ATTRIBUTO_DESCRIPTION,
            _TEST_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            AttributoTypeInt(),
        )

        attributi = await db.retrieve_attributi(
            conn, beamtime_id, associated_table=None
        )

        experiment_type_id = await db.create_experiment_type(
            conn,
            name=_TEST_EXPERIMENT_TYPE_NAME,
            beamtime_id=beamtime_id,
            experiment_attributi=[
                AttributoIdAndRole(test_attributo_id, ChemicalType.CRYSTAL)
            ],
        )

        run_internal_id = await db.create_run(
            conn,
            beamtime_id=beamtime_id,
            started=datetime.datetime.utcnow(),
            run_external_id=_TEST_RUN_ID,
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_raw(
                attributi,
                {test_attributo_id: 1},
            ),
            experiment_type_id=experiment_type_id,
            keep_manual_attributes_from_previous_run=False,
        )
        chemical_id = await db.create_chemical(
            conn=conn,
            name=_TEST_CHEMICAL_NAME,
            beamtime_id=beamtime_id,
            responsible_person=_TEST_CHEMICAL_RESPONSIBLE_PERSON,
            type_=ChemicalType.CRYSTAL,
            attributi=AttributiMap.from_types_and_json_dict(
                attributi,
                json_dict={},
            ),
        )

        frames = 10
        hits = 11
        not_indexed_frames = 12
        point_group = "4/mmm"
        cell_description = parse_cell_description(
            "tetragonal P c (79.2 79.2 38.0) (90 90 90)"
        )
        assert cell_description is not None
        indexing_result_id = await db.create_indexing_result(
            conn,
            DBIndexingResultInput(
                created=datetime.datetime.utcnow(),
                run_id=run_internal_id,
                frames=frames,
                hits=hits,
                not_indexed_frames=not_indexed_frames,
                chemical_id=chemical_id,
                cell_description=cell_description,  # type: ignore
                point_group=point_group,
                runtime_status=None,
            ),
        )

        assert (await db.retrieve_indexing_result(conn, indexing_result_id)) is not None
        indexing_results = await db.retrieve_indexing_results(conn, beamtime_id)

        assert len(indexing_results) == 1
        assert indexing_results[0].chemical_id == chemical_id
        assert indexing_results[0].frames == frames
        assert indexing_results[0].hits == hits
        assert indexing_results[0].not_indexed_frames == not_indexed_frames

        parameters = DBMergeParameters(
            point_group=point_group,
            cell_description=cell_description,
            negative_handling=None,
            merge_model=MergeModel.UNITY,
            scale_intensities=ScaleIntensities.OFF,
            post_refinement=False,
            iterations=1,
            polarisation=None,
            start_after=None,
            stop_after=None,
            rel_b=1.5,
            no_pr=True,
            force_bandwidth=None,
            force_radius=None,
            force_lambda=None,
            no_delta_cc_half=True,
            max_adu=None,
            min_measurements=1,
            logs=True,
            min_res=None,
            push_res=None,
            w=None,
        )
        mtz_file_id = (
            await db.create_file(
                conn,
                "test-mtz",
                "my description",
                original_path=None,
                contents_location=Path(__file__).parent / "test-file-no-newlines.txt",
                deduplicate=False,
            )
        ).id
        pdb_file_id = (
            await db.create_file(
                conn,
                "test-pdb",
                "my description",
                original_path=None,
                contents_location=Path(__file__).parent / "test-file-no-newlines.txt",
                deduplicate=False,
            )
        ).id

        merge_result_runtime_status = DBMergeRuntimeStatusDone(
            started=datetime.datetime.utcnow(),
            stopped=datetime.datetime.utcnow(),
            result=MergeResult(
                mtz_file_id=mtz_file_id,
                fom=MergeResultFom(
                    snr=1.0,
                    wilson=None,
                    ln_k=None,
                    discarded_reflections=1,
                    one_over_d_from=1.1,
                    one_over_d_to=1.2,
                    redundancy=1.3,
                    completeness=1.4,
                    measurements_total=10,
                    reflections_total=11,
                    reflections_possible=12,
                    r_split=1.5,
                    r1i=1.6,
                    r2=1.7,
                    cc=1.8,
                    ccstar=1.9,
                    ccano=2.0,
                    crdano=2.1,
                    rano=2.2,
                    rano_over_r_split=2.3,
                    d1sig=2.4,
                    d2sig=2.5,
                    outer_shell=MergeResultOuterShell(
                        resolution=2.6,
                        ccstar=2.7,
                        r_split=2.8,
                        cc=2.9,
                        unique_reflections=20,
                        completeness=3.0,
                        redundancy=3.1,
                        snr=3.2,
                        min_res=3.3,
                        max_res=3.4,
                    ),
                ),
                detailed_foms=[
                    MergeResultShell(
                        one_over_d_centre=4.0,
                        nref=3,
                        d_over_a=4.1,
                        min_res=4.2,
                        max_res=4.3,
                        cc=4.4,
                        ccstar=4.5,
                        r_split=4.6,
                        reflections_possible=4,
                        completeness=4.7,
                        measurements=5,
                        redundancy=4.8,
                        snr=4.9,
                        mean_i=5.0,
                    )
                ],
                refinement_results=[],
            ),
            recent_log="log",
        )
        merge_result_id = await db.create_merge_result(
            conn,
            DBMergeResultInput(
                created=datetime.datetime.utcnow(),
                indexing_results=indexing_results,
                parameters=parameters,
                runtime_status=merge_result_runtime_status,
            ),
        )
        # create_merge_result either returns the ID or an error object, so check that it worked here
        assert isinstance(merge_result_id, int)

        merge_results = await db.retrieve_merge_results(conn)
        assert len(merge_results) == 1
        assert merge_results[0].id == merge_result_id
        assert merge_results[0].parameters == parameters
        assert merge_results[0].runtime_status == merge_result_runtime_status

        refinement_result_id = await db.create_refinement_result(
            conn,
            merge_result_id,
            pdb_file_id,
            mtz_file_id,
            rfree=0.5,
            rwork=0.6,
            rms_bond_angle=1.0,
            rms_bond_length=2.0,
        )

        refinement_results = await db.retrieve_refinement_results(conn)

        assert len(refinement_results) == 1
        # Smoke test for now, we could check all the parameters at some point
        assert refinement_results[0].merge_result_id == merge_result_id
        assert refinement_results[0].id == refinement_result_id
