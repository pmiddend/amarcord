from datetime import datetime

import pytest

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.attributo_type import (
    AttributoTypeInt,
    AttributoTypeString,
    AttributoTypeBoolean,
    AttributoTypeDecimal,
    AttributoType,
    AttributoTypeSample,
    AttributoTypeDateTime,
)
from amarcord.db.dbattributo import DBAttributo
from amarcord.numeric_range import NumericRange

TEST_ATTRIBUTO_ID = "test"


def _create_attributo(t: AttributoType) -> DBAttributo:
    return DBAttributo(
        TEST_ATTRIBUTO_ID,
        "description",
        "manual",
        AssociatedTable.RUN,
        t,
    )


def test_attributi_map_check_type_for_sample() -> None:
    with pytest.raises(Exception):
        # Give a wrong sample ID for a sample attributo, should not work
        AttributiMap.from_types_and_json(
            [_create_attributo(AttributoTypeSample())],
            [1],
            {TEST_ATTRIBUTO_ID: 2},
        )

    # Give a valid sample ID for a sample attributo, should work
    am = AttributiMap.from_types_and_json(
        [_create_attributo(AttributoTypeSample())],
        [1],
        {TEST_ATTRIBUTO_ID: 1},
    )

    # Then try to retrieve the attributo
    assert am.select(TEST_ATTRIBUTO_ID) == 1


def test_attributi_map_check_type_for_int() -> None:
    # Give a string for an integer attributo, should fail
    with pytest.raises(Exception):
        AttributiMap.from_types_and_json(
            [_create_attributo(AttributoTypeInt())],
            [],
            {TEST_ATTRIBUTO_ID: "foo"},
        )

    # Give a proper integer for an integer attributo, should work
    am = AttributiMap.from_types_and_json(
        [_create_attributo(AttributoTypeInt())],
        [],
        {TEST_ATTRIBUTO_ID: 3},
    )

    # Then try to retrieve the attributo
    assert am.select(TEST_ATTRIBUTO_ID) == 3
    assert am.select_int(TEST_ATTRIBUTO_ID) == 3
    assert am.select_int_unsafe(TEST_ATTRIBUTO_ID) == 3

    # En passant, try to retrieve something invalid
    assert am.select("invalid") is None
    assert am.select_int("invalid") is None
    with pytest.raises(Exception):
        assert am.select_int_unsafe("invalid") is None


def test_attributi_map_check_type_for_string() -> None:
    # Give an integer for a string attributo, should fail
    with pytest.raises(Exception):
        AttributiMap.from_types_and_json(
            [_create_attributo(AttributoTypeString())],
            [],
            {TEST_ATTRIBUTO_ID: 3},
        )

    # Give a proper integer for an integer attributo, should work
    am = AttributiMap.from_types_and_json(
        [_create_attributo(AttributoTypeString())],
        [],
        {TEST_ATTRIBUTO_ID: "foo"},
    )

    # Then try to retrieve the attributo
    assert am.select(TEST_ATTRIBUTO_ID) == "foo"
    assert am.select_string(TEST_ATTRIBUTO_ID) == "foo"

    # En passant, try to retrieve something invalid
    assert am.select("invalid") is None
    assert am.select_string("invalid") is None


def test_attributi_map_check_type_for_boolean() -> None:
    # Give an integer for a boolean attributo, should fail
    with pytest.raises(Exception):
        AttributiMap.from_types_and_json(
            [_create_attributo(AttributoTypeBoolean())],
            [],
            {TEST_ATTRIBUTO_ID: 3},
        )

    # Give a proper integer for an integer attributo, should work
    am = AttributiMap.from_types_and_json(
        [_create_attributo(AttributoTypeBoolean())],
        [],
        {TEST_ATTRIBUTO_ID: True},
    )

    # Then try to retrieve the attributo
    # pylint: disable=singleton-comparison
    assert am.select(TEST_ATTRIBUTO_ID) == True


def test_attributi_map_check_type_for_double() -> None:
    # Give an string for a double attributo, should fail
    with pytest.raises(Exception):
        AttributiMap.from_types_and_json(
            [_create_attributo(AttributoTypeDecimal())],
            [],
            {TEST_ATTRIBUTO_ID: "foo"},
        )

    # Give a proper double for an double attributo, should work
    am = AttributiMap.from_types_and_json(
        [_create_attributo(AttributoTypeDecimal())],
        [],
        {TEST_ATTRIBUTO_ID: 4.5},
    )

    assert am.select(TEST_ATTRIBUTO_ID) == 4.5

    # Give a value that is out of range
    with pytest.raises(Exception):
        AttributiMap.from_types_and_json(
            [
                _create_attributo(
                    AttributoTypeDecimal(range=NumericRange(1.0, False, 2.0, False))
                )
            ],
            [],
            {TEST_ATTRIBUTO_ID: 4.5},
        )


def test_attributi_map_check_type_for_datetime() -> None:
    assert (
        AttributiMap.from_types_and_json(
            [_create_attributo(AttributoTypeDateTime())],
            [],
            {TEST_ATTRIBUTO_ID: 1644317029000},
        ).select_datetime_unsafe(TEST_ATTRIBUTO_ID)
        == datetime(2022, 2, 8, 10, 43, 49)
    )
