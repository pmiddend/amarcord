import datetime
from typing import Any

import pytest

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoType
from amarcord.db.attributo_type import AttributoTypeBoolean
from amarcord.db.attributo_type import AttributoTypeChemical
from amarcord.db.attributo_type import AttributoTypeDecimal
from amarcord.db.attributo_type import AttributoTypeList
from amarcord.db.attributo_type import AttributoTypeString
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.run_external_id import RunExternalId
from amarcord.db.run_internal_id import RunInternalId
from amarcord.db.table_classes import DBRunOutput
from amarcord.filter_expression import FilterInput
from amarcord.filter_expression import compile_run_filter


@pytest.mark.parametrize(
    "filter_expression, run_id, attributi_types, attributi, result",
    [
        ("", 2, {}, {}, True),
        ("id > 1", 2, {}, {}, True),
        ("id > 1", 1, {}, {}, False),
        ('id > "3"', 1, {}, {}, Exception),
        (
            "chemical_name == 1",
            1,
            {"chemical_name": AttributoTypeChemical()},
            {"chemical_name": 1},
            True,
        ),
        (
            '"chemical name" == 1',
            1,
            {"chemical name": AttributoTypeChemical()},
            {"chemical name": 1},
            True,
        ),
        (
            'chemical_name == "lyso"',
            1,
            {"chemical_name": AttributoTypeChemical()},
            {"chemical_name": 1},
            True,
        ),
        (
            'chemical_name == "lyso22"',
            1,
            {"chemical_name": AttributoTypeChemical()},
            {"chemical_name": 1},
            Exception,
        ),
        ("foo > 3.0", 1, {"foo": AttributoTypeDecimal()}, {"foo": 4.0}, True),
        ("foo < 3.0", 1, {"foo": AttributoTypeDecimal()}, {"foo": 4.0}, False),
        ("foo != None", 1, {"foo": AttributoTypeDecimal()}, {"foo": 4.0}, True),
        ("foo == None", 1, {"foo": AttributoTypeDecimal()}, {"foo": 4.0}, False),
        ("foo == True", 1, {"foo": AttributoTypeBoolean()}, {"foo": True}, True),
        ("foo == False", 1, {"foo": AttributoTypeBoolean()}, {"foo": True}, False),
        (
            "foo > 3.0 and foo < 5.0",
            1,
            {"foo": AttributoTypeDecimal()},
            {"foo": 4.0},
            True,
        ),
        (
            "foo > 3.0 and foo < 5.0",
            1,
            {"foo": AttributoTypeDecimal()},
            {"foo": 6.0},
            False,
        ),
        (
            "foo > 3.0 or foo < -10.0",
            1,
            {"foo": AttributoTypeDecimal()},
            {"foo": 4.0},
            True,
        ),
        (
            "foo > 3.0 or foo < -10.0",
            1,
            {"foo": AttributoTypeDecimal()},
            {"foo": -20.0},
            True,
        ),
        (
            'bar == "x" and (foo > 3.0 and foo < 10.0)',
            1,
            {"foo": AttributoTypeDecimal(), "bar": AttributoTypeString()},
            {"foo": 5.0, "bar": "x"},
            True,
        ),
        (
            'bar == "x" and (foo > 3.0 and foo < 10.0)',
            1,
            {"foo": AttributoTypeDecimal(), "bar": AttributoTypeString()},
            {"foo": 5.0, "bar": "y"},
            False,
        ),
        (
            'bar == "x" and (foo > 3.0 and foo < 10.0)',
            1,
            {"foo": AttributoTypeDecimal(), "bar": AttributoTypeString()},
            {"foo": 11, "bar": "x"},
            False,
        ),
        (
            'foo ? "x"',
            1,
            {
                "foo": AttributoTypeList(
                    sub_type=AttributoTypeString(), min_length=None, max_length=None
                )
            },
            {"foo": ["x", "a"]},
            True,
        ),
        (
            'foo ? "y"',
            1,
            {
                "foo": AttributoTypeList(
                    sub_type=AttributoTypeString(), min_length=None, max_length=None
                )
            },
            {"foo": ["x", "a"]},
            False,
        ),
    ],
)
def test_compile_run_filter(
    filter_expression: str,
    run_id: RunInternalId,
    attributi_types: dict[str, AttributoType],
    attributi: dict[str, Any],
    result: bool | Exception,
) -> None:
    running_id = 1
    attributo_name_to_id: dict[str, AttributoId] = {}
    for attributo_name in attributi_types.keys():
        attributo_name_to_id[attributo_name] = AttributoId(running_id)
        running_id += 1

    beamtime_id = BeamtimeId(1)
    attributi_map = AttributiMap(
        types_dict={
            attributo_name_to_id[key]: DBAttributo(
                id=attributo_name_to_id[key],
                beamtime_id=beamtime_id,
                name=key,
                description="",
                group="",
                associated_table=AssociatedTable.RUN,
                attributo_type=value,
            )
            for key, value in attributi_types.items()
        },
        impl={attributo_name_to_id[key]: value for key, value in attributi.items()},
    )
    filter_input = FilterInput(
        run=DBRunOutput(
            id=run_id,
            external_id=RunExternalId(int(run_id)),
            beamtime_id=beamtime_id,
            attributi=attributi_map,
            experiment_type_id=0,
            files=[],
            started=datetime.datetime.now(),
            stopped=datetime.datetime.now(),
        ),
        chemical_names={"lyso": 1},
        attributo_name_to_id=attributo_name_to_id,
    )
    if isinstance(result, bool):
        assert compile_run_filter(filter_expression)(filter_input) == result
    else:
        with pytest.raises(Exception):
            compile_run_filter(filter_expression)(filter_input)
