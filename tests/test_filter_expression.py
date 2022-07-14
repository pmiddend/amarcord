from typing import Any

import pytest

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import (
    AttributoType,
    AttributoTypeDecimal,
    AttributoTypeString,
    AttributoTypeList,
    AttributoTypeBoolean,
    AttributoTypeSample,
)
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.table_classes import DBRun
from amarcord.filter_expression import compile_run_filter, FilterInput


@pytest.mark.parametrize(
    "filter_expression, run_id, attributi_types, attributi, result",
    [
        ("", 2, {}, {}, True),
        ("id > 1", 2, {}, {}, True),
        ("id > 1", 1, {}, {}, False),
        ('id > "3"', 1, {}, {}, Exception),
        (
            "sample_name == 1",
            1,
            {"sample_name": AttributoTypeSample()},
            {"sample_name": 1},
            True,
        ),
        (
            '"sample name" == 1',
            1,
            {"sample name": AttributoTypeSample()},
            {"sample name": 1},
            True,
        ),
        (
            'sample_name == "lyso"',
            1,
            {"sample_name": AttributoTypeSample()},
            {"sample_name": 1},
            True,
        ),
        (
            'sample_name == "lyso22"',
            1,
            {"sample_name": AttributoTypeSample()},
            {"sample_name": 1},
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
    run_id: int,
    attributi_types: dict[str, AttributoType],
    attributi: dict[str, Any],
    result: bool | Exception,
) -> None:
    attributi_map = AttributiMap(
        types_dict={
            AttributoId(key): DBAttributo(
                AttributoId(key),
                "",
                "",
                AssociatedTable.RUN,
                value,
            )
            for key, value in attributi_types.items()
        },
        sample_ids=[1],
        impl={AttributoId(key): value for key, value in attributi.items()},
    )
    filter_input = FilterInput(
        run=DBRun(
            run_id,
            attributi_map,
            files=[],
        ),
        sample_names={"lyso": 1},
    )
    if isinstance(result, bool):
        assert compile_run_filter(filter_expression)(filter_input) == result
    else:
        with pytest.raises(Exception):
            compile_run_filter(filter_expression)(filter_input)
