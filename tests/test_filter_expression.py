import datetime
from typing import Any

import pytest

from amarcord.db import orm
from amarcord.db.attributi import attributo_type_to_schema
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import ArrayAttributoType
from amarcord.db.attributo_type import AttributoType
from amarcord.db.attributo_type import AttributoTypeBoolean
from amarcord.db.attributo_type import AttributoTypeChemical
from amarcord.db.attributo_type import AttributoTypeDecimal
from amarcord.db.attributo_type import AttributoTypeList
from amarcord.db.attributo_type import AttributoTypeString
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.run_external_id import RunExternalId
from amarcord.db.run_internal_id import RunInternalId
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
                    sub_type=ArrayAttributoType.ARRAY_STRING,
                    min_length=None,
                    max_length=None,
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
                    sub_type=ArrayAttributoType.ARRAY_STRING,
                    min_length=None,
                    max_length=None,
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
    filter_input = FilterInput(
        run=orm.Run(
            id=run_id,
            external_id=RunExternalId(int(run_id)),
            beamtime_id=beamtime_id,
            experiment_type_id=0,
            started=datetime.datetime.now(),
            stopped=datetime.datetime.now(),
            attributo_values=[
                (
                    orm.RunHasAttributoValue(
                        attributo_id=attributo_name_to_id[attributo_name],
                        chemical_value=value,
                        attributo=orm.Attributo(
                            json_schema=attributo_type_to_schema(
                                attributi_types[attributo_name]
                            )
                        ),
                    )
                    if isinstance(
                        attributi_types[attributo_name], AttributoTypeChemical
                    )
                    else (
                        orm.RunHasAttributoValue(
                            attributo_id=attributo_name_to_id[attributo_name],
                            float_value=value,
                            attributo=orm.Attributo(
                                json_schema=attributo_type_to_schema(
                                    attributi_types[attributo_name]
                                )
                            ),
                        )
                        if isinstance(
                            attributi_types[attributo_name], AttributoTypeDecimal
                        )
                        else (
                            orm.RunHasAttributoValue(
                                attributo_id=attributo_name_to_id[attributo_name],
                                bool_value=value,
                                attributo=orm.Attributo(
                                    json_schema=attributo_type_to_schema(
                                        attributi_types[attributo_name]
                                    )
                                ),
                            )
                            if isinstance(
                                attributi_types[attributo_name], AttributoTypeBoolean
                            )
                            else orm.RunHasAttributoValue(
                                attributo_id=attributo_name_to_id[attributo_name],
                                list_value=value,
                                attributo=orm.Attributo(
                                    json_schema=attributo_type_to_schema(
                                        attributi_types[attributo_name]
                                    )
                                ),
                            )
                        )
                    )
                )
                for attributo_name, value in attributi.items()
            ],
        ),
        chemical_names={"lyso": 1},
        attributo_name_to_id=attributo_name_to_id,
    )
    if isinstance(result, bool):
        assert compile_run_filter(filter_expression)(filter_input) == result
    else:
        with pytest.raises(Exception):
            compile_run_filter(filter_expression)(filter_input)
