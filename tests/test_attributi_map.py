from datetime import datetime
from typing import List, cast

import pytest

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.asyncdb import ATTRIBUTO_GROUP_MANUAL
from amarcord.db.attributi_map import (
    AttributiMap,
    SPECIAL_SAMPLE_ID_NONE,
    SPECIAL_VALUE_CHOICE_NONE,
    run_matches_dataset,
)
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import (
    AttributoTypeInt,
    AttributoTypeString,
    AttributoTypeBoolean,
    AttributoTypeDecimal,
    AttributoType,
    AttributoTypeSample,
    AttributoTypeDateTime,
    AttributoTypeChoice,
    AttributoTypeList,
)
from amarcord.db.dbattributo import DBAttributo
from amarcord.numeric_range import NumericRange

TEST_ATTRIBUTO_ID = AttributoId("test")


def _create_named_attributo(name: AttributoId, t: AttributoType) -> DBAttributo:
    return DBAttributo(
        name,
        "description",
        ATTRIBUTO_GROUP_MANUAL,
        AssociatedTable.RUN,
        t,
    )


def _create_attributo(t: AttributoType) -> DBAttributo:
    return _create_named_attributo(TEST_ATTRIBUTO_ID, t)


def test_attributi_map_checks_superfluous_attributi() -> None:
    with pytest.raises(Exception):
        # If we have an attributo without a corresponding type, this is an error
        AttributiMap.from_types_and_json(
            [_create_attributo(AttributoTypeInt())],
            [1],
            {TEST_ATTRIBUTO_ID: 2, TEST_ATTRIBUTO_ID + "unknown": 3},
        )


def test_attributi_map_accepts_nulls() -> None:
    amap = AttributiMap.from_types_and_json(
        [_create_attributo(AttributoTypeInt())],
        [1],
        {TEST_ATTRIBUTO_ID: None},
    )
    assert amap.select(TEST_ATTRIBUTO_ID) is None


def test_attributi_map_empty_choice_string_is_none() -> None:
    amap = AttributiMap.from_types_and_json(
        [_create_attributo(AttributoTypeChoice(values=["a", "ab"]))],
        [1],
        {TEST_ATTRIBUTO_ID: SPECIAL_VALUE_CHOICE_NONE},
    )
    assert amap.select(TEST_ATTRIBUTO_ID) == SPECIAL_VALUE_CHOICE_NONE


def test_attributi_map_invalid_choice_value() -> None:
    with pytest.raises(Exception):
        AttributiMap.from_types_and_json(
            [_create_attributo(AttributoTypeChoice(values=["a", "ab"]))],
            [1],
            # value "c" is not a valid choice
            {TEST_ATTRIBUTO_ID: "c"},
        )


def test_attributi_map_valid_choice_value() -> None:
    amap = AttributiMap.from_types_and_json(
        [_create_attributo(AttributoTypeChoice(values=["a", "ab"]))],
        [1],
        {TEST_ATTRIBUTO_ID: "ab"},
    )

    assert amap.select_string(TEST_ATTRIBUTO_ID) == "ab"


def test_attributi_map_equality_check() -> None:
    amap = AttributiMap.from_types_and_json(
        [
            _create_attributo(
                AttributoTypeList(
                    sub_type=AttributoTypeString(), min_length=None, max_length=None
                )
            )
        ],
        [1],
        {TEST_ATTRIBUTO_ID: ["a"]},
    )
    bmap = AttributiMap.from_types_and_json(
        [
            _create_attributo(
                AttributoTypeList(
                    sub_type=AttributoTypeString(), min_length=None, max_length=None
                )
            )
        ],
        [1],
        {TEST_ATTRIBUTO_ID: ["b"]},
    )

    assert amap != bmap


def test_attributi_map_valid_list_value() -> None:
    amap = AttributiMap.from_types_and_json(
        [
            _create_attributo(
                AttributoTypeList(
                    sub_type=AttributoTypeString(), min_length=None, max_length=None
                )
            )
        ],
        [1],
        {TEST_ATTRIBUTO_ID: ["a"]},
    )

    assert amap.select(TEST_ATTRIBUTO_ID) == ["a"]


def test_attributi_map_to_json_with_list() -> None:
    result = AttributiMap.from_types_and_json(
        [
            _create_attributo(
                AttributoTypeList(
                    sub_type=AttributoTypeString(), min_length=None, max_length=None
                )
            )
        ],
        [1],
        {TEST_ATTRIBUTO_ID: ["a", "b", "c"]},
    ).to_json()

    assert result[TEST_ATTRIBUTO_ID] == ["a", "b", "c"]


def test_attributi_map_invalid_list_length_too_big() -> None:
    with pytest.raises(Exception):
        AttributiMap.from_types_and_json(
            [
                _create_attributo(
                    AttributoTypeList(
                        sub_type=AttributoTypeString(), min_length=None, max_length=2
                    )
                )
            ],
            [1],
            {TEST_ATTRIBUTO_ID: ["a", "b", "c"]},
        )


def test_attributi_map_invalid_list_length_too_small() -> None:
    with pytest.raises(Exception):
        AttributiMap.from_types_and_json(
            [
                _create_attributo(
                    AttributoTypeList(
                        sub_type=AttributoTypeString(), min_length=2, max_length=None
                    )
                )
            ],
            [1],
            {TEST_ATTRIBUTO_ID: ["a"]},
        )


def test_attributi_map_invalid_list_value() -> None:
    with pytest.raises(Exception):
        AttributiMap.from_types_and_json(
            [
                _create_attributo(
                    AttributoTypeList(
                        sub_type=AttributoTypeString(), min_length=None, max_length=None
                    )
                )
            ],
            [1],
            {TEST_ATTRIBUTO_ID: [1]},
        )


def test_attributi_map_sample_id_zero_is_none() -> None:
    amap = AttributiMap.from_types_and_json(
        [_create_attributo(AttributoTypeSample())],
        [1],
        {TEST_ATTRIBUTO_ID: SPECIAL_SAMPLE_ID_NONE},
    )
    assert amap.select(TEST_ATTRIBUTO_ID) == SPECIAL_SAMPLE_ID_NONE


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
    assert am.select(AttributoId("invalid")) is None
    assert am.select_int(AttributoId("invalid")) is None
    with pytest.raises(Exception):
        assert am.select_int_unsafe(AttributoId("invalid")) is None


def test_check_attributo_types_when_extending_string() -> None:
    m = AttributiMap.from_types_and_json(
        [_create_attributo(AttributoTypeString())],
        [],
        {},
    )

    m.extend({TEST_ATTRIBUTO_ID: "a"})

    with pytest.raises(Exception):
        m.extend({TEST_ATTRIBUTO_ID: 3})


def test_check_attributo_types_when_extending_int() -> None:
    m = AttributiMap.from_types_and_json(
        [_create_attributo(AttributoTypeInt())],
        [],
        {},
    )

    with pytest.raises(Exception):
        m.extend({TEST_ATTRIBUTO_ID: "a"})
    m.extend({TEST_ATTRIBUTO_ID: 1})


def test_check_attributo_types_when_extending_boolean() -> None:
    m = AttributiMap.from_types_and_json(
        [_create_attributo(AttributoTypeBoolean())],
        [],
        {},
    )

    with pytest.raises(Exception):
        m.extend({TEST_ATTRIBUTO_ID: "a"})
    m.extend({TEST_ATTRIBUTO_ID: True})


def test_check_attributo_types_when_extending_decimal_without_range() -> None:
    m = AttributiMap.from_types_and_json(
        [
            _create_attributo(
                AttributoTypeDecimal(range=None, suffix=None, standard_unit=False)
            )
        ],
        [],
        {},
    )

    with pytest.raises(Exception):
        m.extend({TEST_ATTRIBUTO_ID: "a"})
    m.extend({TEST_ATTRIBUTO_ID: 1.0})


def test_check_attributo_types_when_extending_decimal_with_range() -> None:
    m = AttributiMap.from_types_and_json(
        [
            _create_attributo(
                AttributoTypeDecimal(
                    range=NumericRange(
                        minimum=0,
                        minimum_inclusive=False,
                        maximum=1,
                        maximum_inclusive=True,
                    ),
                    suffix=None,
                    standard_unit=False,
                )
            )
        ],
        [],
        {},
    )

    with pytest.raises(Exception):
        # String instead of decimal
        m.extend({TEST_ATTRIBUTO_ID: "a"})
    with pytest.raises(Exception):
        # Out of range
        m.extend({TEST_ATTRIBUTO_ID: 10.0})
    # 1.0 is inclusive
    m.extend({TEST_ATTRIBUTO_ID: 1.0})
    m.extend({TEST_ATTRIBUTO_ID: 0.5})


def test_check_attributo_types_when_extending_choice() -> None:
    m = AttributiMap.from_types_and_json(
        [_create_attributo(AttributoTypeChoice(values=["a", "b"]))],
        [],
        {},
    )

    with pytest.raises(Exception):
        # invalid choice
        m.extend({TEST_ATTRIBUTO_ID: "x"})
    with pytest.raises(Exception):
        # invalid type
        m.extend({TEST_ATTRIBUTO_ID: 2})
    # valid choice
    m.extend({TEST_ATTRIBUTO_ID: "a"})


def test_check_attributo_types_when_extending_datetime() -> None:
    m = AttributiMap.from_types_and_json(
        [_create_attributo(AttributoTypeDateTime())],
        [],
        {},
    )

    with pytest.raises(Exception):
        # invalid type
        m.extend({TEST_ATTRIBUTO_ID: "x"})
    # valid choice (though unlikely)
    m.extend({TEST_ATTRIBUTO_ID: datetime.utcnow()})


def test_check_attributo_types_when_extending_list() -> None:
    m = AttributiMap.from_types_and_json(
        [
            _create_attributo(
                AttributoTypeList(
                    sub_type=AttributoTypeString(), min_length=1, max_length=2
                )
            )
        ],
        [],
        {},
    )

    with pytest.raises(Exception):
        # invalid type
        m.extend({TEST_ATTRIBUTO_ID: [1]})

    with pytest.raises(Exception):
        # invalid length
        m.extend({TEST_ATTRIBUTO_ID: cast(List[str], [])})

    with pytest.raises(Exception):
        # invalid length
        m.extend({TEST_ATTRIBUTO_ID: ["a", "b", "c"]})

    m.extend({TEST_ATTRIBUTO_ID: ["a"]})


def test_check_attributo_types_when_extending_non_existing_attributo() -> None:
    with pytest.raises(Exception):
        AttributiMap.from_types_and_json(
            [],
            [1],
            {"foo": 1},
        )


def test_check_attributo_types_when_extending_sample() -> None:
    m = AttributiMap.from_types_and_json(
        [_create_attributo(AttributoTypeSample())],
        # Only one sample allowed: 1
        [1],
        {},
    )

    with pytest.raises(Exception):
        m.extend({TEST_ATTRIBUTO_ID: "a"})
    with pytest.raises(Exception):
        # Sample 3 isn't valid
        m.extend({TEST_ATTRIBUTO_ID: 3})
    # Sample 1 valid
    m.extend({TEST_ATTRIBUTO_ID: 1})


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
    assert am.select(AttributoId("invalid")) is None
    assert am.select_string(AttributoId("invalid")) is None


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
    assert AttributiMap.from_types_and_json(
        [_create_attributo(AttributoTypeDateTime())],
        [],
        {TEST_ATTRIBUTO_ID: 1644317029000},
    ).select_datetime_unsafe(TEST_ATTRIBUTO_ID) == datetime(2022, 2, 8, 10, 43, 49)


def test_extend_with_attributi_map() -> None:
    a = AttributiMap.from_types_and_json(
        [_create_named_attributo(AttributoId("a"), AttributoTypeString())],
        [],
        {AttributoId("a"): "foo"},
    )
    b = AttributiMap.from_types_and_json(
        [_create_named_attributo(AttributoId("b"), AttributoTypeInt())],
        [],
        {AttributoId("b"): 3},
    )
    a.extend_with_attributi_map(b)
    assert a.select_string(AttributoId("a")) == "foo"
    assert a.select_int(AttributoId("b")) == 3


def test_create_sub_map_for_group() -> None:
    a = AttributiMap.from_types_and_json(
        [
            DBAttributo(
                AttributoId("a"),
                "description",
                ATTRIBUTO_GROUP_MANUAL,
                AssociatedTable.RUN,
                AttributoTypeInt(),
            ),
            DBAttributo(
                AttributoId("b"),
                "description",
                "automatic",
                AssociatedTable.RUN,
                AttributoTypeString(),
            ),
        ],
        [],
        {AttributoId("a"): 3, AttributoId("b"): "foo"},
    ).create_sub_map_for_group(ATTRIBUTO_GROUP_MANUAL)

    assert a.select_int(AttributoId("a")) == 3
    assert a.select(AttributoId("b")) is None


def test_run_matches_dataset_bool_and_string() -> None:
    a = AttributoId("a")
    b = AttributoId("b")
    attributi = (
        DBAttributo(
            a,
            "description",
            ATTRIBUTO_GROUP_MANUAL,
            AssociatedTable.RUN,
            AttributoTypeBoolean(),
        ),
        DBAttributo(
            b,
            "description",
            "automatic",
            AssociatedTable.RUN,
            AttributoTypeString(),
        ),
    )
    run_attributi_no_boolean = AttributiMap.from_types_and_json(
        attributi,
        [],
        # Note: boolean is missing here!
        {b: "foo"},
    )
    run_attributi_boolean_false = AttributiMap.from_types_and_json(
        attributi,
        [],
        # Note: boolean is false here!
        {a: False, b: "foo"},
    )
    run_attributi_boolean_true = AttributiMap.from_types_and_json(
        attributi,
        [],
        {a: True, b: "foo"},
    )
    data_set_attributi_boolean_false = AttributiMap.from_types_and_json(
        attributi,
        [],
        # Boolean has to be false (or missing!)
        {a: False, b: "foo"},
    )
    data_set_attributi_boolean_true = AttributiMap.from_types_and_json(
        attributi,
        [],
        {a: True, b: "foo"},
    )
    assert run_matches_dataset(
        run_attributi=run_attributi_no_boolean,
        data_set_attributi=data_set_attributi_boolean_false,
    )
    assert run_matches_dataset(
        run_attributi=run_attributi_boolean_false,
        data_set_attributi=data_set_attributi_boolean_false,
    )
    assert not run_matches_dataset(
        run_attributi=run_attributi_boolean_true,
        data_set_attributi=data_set_attributi_boolean_false,
    )
    assert not run_matches_dataset(
        run_attributi=run_attributi_boolean_false,
        data_set_attributi=data_set_attributi_boolean_true,
    )
    assert not run_matches_dataset(
        run_attributi=run_attributi_no_boolean,
        data_set_attributi=data_set_attributi_boolean_true,
    )


def test_run_matches_dataset_float() -> None:
    a = AttributoId("a")
    attributi = (
        DBAttributo(
            a,
            "description",
            ATTRIBUTO_GROUP_MANUAL,
            AssociatedTable.RUN,
            AttributoTypeDecimal(),
        ),
    )
    run_attributi_missing = AttributiMap.from_types_and_json(
        attributi,
        [],
        {},
    )
    run_attributi_200 = AttributiMap.from_types_and_json(
        attributi,
        [],
        {a: 200.0},
    )
    data_set_attributi_199 = AttributiMap.from_types_and_json(
        attributi,
        [],
        {a: 199.0},
    )
    data_set_attributi_198 = AttributiMap.from_types_and_json(
        attributi,
        [],
        {a: 198.0},
    )
    # This is testing the rather random relative error comparison value. But better have test than not have it. :D
    assert run_matches_dataset(
        run_attributi=run_attributi_200,
        data_set_attributi=data_set_attributi_199,
    )
    assert not run_matches_dataset(
        run_attributi=run_attributi_200,
        data_set_attributi=data_set_attributi_198,
    )
    assert not run_matches_dataset(
        run_attributi=run_attributi_missing,
        data_set_attributi=data_set_attributi_198,
    )
