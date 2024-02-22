from datetime import datetime
from typing import cast

import pytest

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.asyncdb import ATTRIBUTO_GROUP_MANUAL
from amarcord.db.attributi_map import SPECIAL_VALUE_CHOICE_NONE
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.attributi_map import decimal_attributi_match
from amarcord.db.attributi_map import run_matches_dataset
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoType
from amarcord.db.attributo_type import AttributoTypeBoolean
from amarcord.db.attributo_type import AttributoTypeChemical
from amarcord.db.attributo_type import AttributoTypeChoice
from amarcord.db.attributo_type import AttributoTypeDateTime
from amarcord.db.attributo_type import AttributoTypeDecimal
from amarcord.db.attributo_type import AttributoTypeInt
from amarcord.db.attributo_type import AttributoTypeList
from amarcord.db.attributo_type import AttributoTypeString
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.dbattributo import DBAttributo
from amarcord.numeric_range import NumericRange

TEST_ATTRIBUTO_ID = AttributoId(1)
TEST_ATTRIBUTO_NAME = "test"


def _create_named_attributo(
    id_: AttributoId, name: str, t: AttributoType
) -> DBAttributo:
    return DBAttributo(
        id=id_,
        beamtime_id=BeamtimeId(1),
        name=name,
        description="description",
        group=ATTRIBUTO_GROUP_MANUAL,
        associated_table=AssociatedTable.RUN,
        attributo_type=t,
    )


def _create_attributo(t: AttributoType) -> DBAttributo:
    return _create_named_attributo(id_=TEST_ATTRIBUTO_ID, name=TEST_ATTRIBUTO_NAME, t=t)


def test_attributi_map_checks_superfluous_attributi() -> None:
    with pytest.raises(Exception):
        # If we have an attributo without a corresponding type, this is an error
        AttributiMap.from_types_and_json_dict(
            [_create_attributo(AttributoTypeInt())],
            {str(TEST_ATTRIBUTO_ID): 2, str(TEST_ATTRIBUTO_ID + 1): 3},
        )


def test_attributi_map_accepts_nulls() -> None:
    amap = AttributiMap.from_types_and_json_dict(
        [_create_attributo(AttributoTypeInt())],
        {str(TEST_ATTRIBUTO_ID): None},
    )
    assert amap.select(TEST_ATTRIBUTO_ID) is None


def test_attributi_map_empty_choice_string_is_none() -> None:
    amap = AttributiMap.from_types_and_json_dict(
        [_create_attributo(AttributoTypeChoice(values=["a", "ab"]))],
        {str(TEST_ATTRIBUTO_ID): SPECIAL_VALUE_CHOICE_NONE},
    )
    assert amap.select(TEST_ATTRIBUTO_ID) == SPECIAL_VALUE_CHOICE_NONE


def test_attributi_map_invalid_choice_value() -> None:
    with pytest.raises(Exception):
        AttributiMap.from_types_and_json_dict(
            [_create_attributo(AttributoTypeChoice(values=["a", "ab"]))],
            # value "c" is not a valid choice
            {str(TEST_ATTRIBUTO_ID): "c"},
        )


def test_attributi_map_valid_choice_value() -> None:
    amap = AttributiMap.from_types_and_json_dict(
        [_create_attributo(AttributoTypeChoice(values=["a", "ab"]))],
        {str(TEST_ATTRIBUTO_ID): "ab"},
    )

    assert amap.select_string(TEST_ATTRIBUTO_ID) == "ab"


def test_attributi_map_equality_check() -> None:
    amap = AttributiMap.from_types_and_json_dict(
        [
            _create_attributo(
                AttributoTypeList(
                    sub_type=AttributoTypeString(), min_length=None, max_length=None
                )
            )
        ],
        {str(TEST_ATTRIBUTO_ID): ["a"]},
    )
    bmap = AttributiMap.from_types_and_json_dict(
        [
            _create_attributo(
                AttributoTypeList(
                    sub_type=AttributoTypeString(), min_length=None, max_length=None
                )
            )
        ],
        {str(TEST_ATTRIBUTO_ID): ["b"]},
    )

    assert amap != bmap


def test_attributi_map_valid_list_value() -> None:
    amap = AttributiMap.from_types_and_json_dict(
        [
            _create_attributo(
                AttributoTypeList(
                    sub_type=AttributoTypeString(), min_length=None, max_length=None
                )
            )
        ],
        {str(TEST_ATTRIBUTO_ID): ["a"]},
    )

    assert amap.select(TEST_ATTRIBUTO_ID) == ["a"]


def test_attributi_map_to_json_with_list() -> None:
    result = AttributiMap.from_types_and_json_dict(
        [
            _create_attributo(
                AttributoTypeList(
                    sub_type=AttributoTypeString(), min_length=None, max_length=None
                )
            )
        ],
        {str(TEST_ATTRIBUTO_ID): ["a", "b", "c"]},
    ).to_json()

    assert result[str(TEST_ATTRIBUTO_ID)] == ["a", "b", "c"]


def test_attributi_map_invalid_list_length_too_big() -> None:
    with pytest.raises(Exception):
        AttributiMap.from_types_and_raw(
            [
                _create_attributo(
                    AttributoTypeList(
                        sub_type=AttributoTypeString(), min_length=None, max_length=2
                    )
                )
            ],
            {TEST_ATTRIBUTO_ID: ["a", "b", "c"]},
        )


def test_attributi_map_invalid_list_length_too_small() -> None:
    with pytest.raises(Exception):
        AttributiMap.from_types_and_raw(
            [
                _create_attributo(
                    AttributoTypeList(
                        sub_type=AttributoTypeString(), min_length=2, max_length=None
                    )
                )
            ],
            {TEST_ATTRIBUTO_ID: ["a"]},
        )


def test_attributi_map_invalid_list_value() -> None:
    with pytest.raises(Exception):
        AttributiMap.from_types_and_raw(
            [
                _create_attributo(
                    AttributoTypeList(
                        sub_type=AttributoTypeString(), min_length=None, max_length=None
                    )
                )
            ],
            {TEST_ATTRIBUTO_ID: [1]},
        )


def test_attributi_map_check_type_for_chemical() -> None:
    # Give a valid chemical ID for a chemical attributo, should work
    am = AttributiMap.from_types_and_raw(
        [_create_attributo(AttributoTypeChemical())],
        {TEST_ATTRIBUTO_ID: 1},
    )

    # Then try to retrieve the attributo
    assert am.select(TEST_ATTRIBUTO_ID) == 1


def test_attributi_map_check_type_for_int() -> None:
    # Give a string for an integer attributo, should fail
    with pytest.raises(Exception):
        AttributiMap.from_types_and_raw(
            [_create_attributo(AttributoTypeInt())],
            {TEST_ATTRIBUTO_ID: "foo"},
        )

    # Give a proper integer for an integer attributo, should work
    am = AttributiMap.from_types_and_raw(
        [_create_attributo(AttributoTypeInt())],
        {TEST_ATTRIBUTO_ID: 3},
    )

    # Then try to retrieve the attributo
    assert am.select(TEST_ATTRIBUTO_ID) == 3
    assert am.select_int(TEST_ATTRIBUTO_ID) == 3
    assert am.select_int_unsafe(TEST_ATTRIBUTO_ID) == 3

    # En passant, try to retrieve something invalid
    invalid_attributo_id = AttributoId(TEST_ATTRIBUTO_ID * 3)
    assert am.select(invalid_attributo_id) is None
    assert am.select_int(invalid_attributo_id) is None
    with pytest.raises(Exception):
        assert am.select_int_unsafe(invalid_attributo_id) is None


def test_check_attributo_types_when_extending_string() -> None:
    m = AttributiMap.from_types_and_raw(
        [_create_attributo(AttributoTypeString())],
        {},
    )

    m.extend({TEST_ATTRIBUTO_ID: "a"})

    with pytest.raises(Exception):
        m.extend({TEST_ATTRIBUTO_ID: 3})


def test_check_attributo_types_when_extending_int() -> None:
    m = AttributiMap.from_types_and_raw(
        [_create_attributo(AttributoTypeInt())],
        {},
    )

    with pytest.raises(Exception):
        m.extend({TEST_ATTRIBUTO_ID: "a"})
    m.extend({TEST_ATTRIBUTO_ID: 1})


def test_check_attributo_types_when_extending_boolean() -> None:
    m = AttributiMap.from_types_and_raw(
        [_create_attributo(AttributoTypeBoolean())],
        {},
    )

    with pytest.raises(Exception):
        m.extend({TEST_ATTRIBUTO_ID: "a"})
    m.extend({TEST_ATTRIBUTO_ID: True})


def test_check_attributo_types_when_extending_decimal_without_range() -> None:
    m = AttributiMap.from_types_and_raw(
        [
            _create_attributo(
                AttributoTypeDecimal(range=None, suffix=None, standard_unit=False)
            )
        ],
        {},
    )

    with pytest.raises(Exception):
        m.extend({TEST_ATTRIBUTO_ID: "a"})
    m.extend({TEST_ATTRIBUTO_ID: 1.0})


def test_check_attributo_types_when_extending_decimal_with_range() -> None:
    m = AttributiMap.from_types_and_raw(
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
    m = AttributiMap.from_types_and_raw(
        [_create_attributo(AttributoTypeChoice(values=["a", "b"]))],
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
    m = AttributiMap.from_types_and_raw(
        [_create_attributo(AttributoTypeDateTime())],
        {},
    )

    with pytest.raises(Exception):
        # invalid type
        m.extend({TEST_ATTRIBUTO_ID: "x"})
    # valid choice (though unlikely)
    m.extend({TEST_ATTRIBUTO_ID: datetime.utcnow()})


def test_check_attributo_types_when_extending_list() -> None:
    m = AttributiMap.from_types_and_raw(
        [
            _create_attributo(
                AttributoTypeList(
                    sub_type=AttributoTypeString(), min_length=1, max_length=2
                )
            )
        ],
        {},
    )

    with pytest.raises(Exception):
        # invalid type
        m.extend({TEST_ATTRIBUTO_ID: [1]})

    with pytest.raises(Exception):
        # invalid length
        m.extend({TEST_ATTRIBUTO_ID: cast(list[str], [])})

    with pytest.raises(Exception):
        # invalid length
        m.extend({TEST_ATTRIBUTO_ID: ["a", "b", "c"]})

    m.extend({TEST_ATTRIBUTO_ID: ["a"]})


def test_check_attributo_types_when_extending_non_existing_attributo() -> None:
    with pytest.raises(Exception):
        AttributiMap.from_types_and_raw(
            [],
            {1: 1},
        )


def test_attributi_map_check_type_for_string() -> None:
    # Give an integer for a string attributo, should fail
    with pytest.raises(Exception):
        AttributiMap.from_types_and_raw(
            [_create_attributo(AttributoTypeString())],
            {TEST_ATTRIBUTO_ID: 3},
        )

    # Give a proper integer for an integer attributo, should work
    am = AttributiMap.from_types_and_raw(
        [_create_attributo(AttributoTypeString())],
        {TEST_ATTRIBUTO_ID: "foo"},
    )

    # Then try to retrieve the attributo
    assert am.select(TEST_ATTRIBUTO_ID) == "foo"
    assert am.select_string(TEST_ATTRIBUTO_ID) == "foo"

    # En passant, try to retrieve something invalid
    invalid_attributo_id = AttributoId(TEST_ATTRIBUTO_ID * 3)
    assert am.select(invalid_attributo_id) is None
    assert am.select_string(invalid_attributo_id) is None


def test_attributi_map_check_type_for_boolean() -> None:
    # Give an integer for a boolean attributo, should fail
    with pytest.raises(Exception):
        AttributiMap.from_types_and_raw(
            [_create_attributo(AttributoTypeBoolean())],
            {TEST_ATTRIBUTO_ID: 3},
        )

    # Give a proper integer for an integer attributo, should work
    am = AttributiMap.from_types_and_raw(
        [_create_attributo(AttributoTypeBoolean())],
        {TEST_ATTRIBUTO_ID: True},
    )

    # Then try to retrieve the attributo
    # pylint: disable=singleton-comparison
    assert am.select(TEST_ATTRIBUTO_ID) == True


def test_attributi_map_check_type_for_double() -> None:
    # Give an string for a double attributo, should fail
    with pytest.raises(Exception):
        AttributiMap.from_types_and_raw(
            [_create_attributo(AttributoTypeDecimal())],
            {TEST_ATTRIBUTO_ID: "foo"},
        )

    # Give a proper double for an double attributo, should work
    am = AttributiMap.from_types_and_raw(
        [_create_attributo(AttributoTypeDecimal())],
        {TEST_ATTRIBUTO_ID: 4.5},
    )

    assert am.select(TEST_ATTRIBUTO_ID) == 4.5

    # Give a value that is out of range
    with pytest.raises(Exception):
        AttributiMap.from_types_and_raw(
            [
                _create_attributo(
                    AttributoTypeDecimal(range=NumericRange(1.0, False, 2.0, False))
                )
            ],
            {TEST_ATTRIBUTO_ID: 4.5},
        )


def test_attributi_map_check_type_for_datetime() -> None:
    assert AttributiMap.from_types_and_json_dict(
        [_create_attributo(AttributoTypeDateTime())],
        {str(TEST_ATTRIBUTO_ID): 1644317029000},
    ).select_datetime_unsafe(TEST_ATTRIBUTO_ID) == datetime(2022, 2, 8, 10, 43, 49)


def test_extend_with_attributi_map() -> None:
    first_id = AttributoId(1)
    second_id = AttributoId(2)

    a = AttributiMap.from_types_and_raw(
        [_create_named_attributo(first_id, "a", AttributoTypeString())],
        {first_id: "foo"},
    )
    b = AttributiMap.from_types_and_raw(
        [_create_named_attributo(second_id, "b", AttributoTypeInt())],
        {second_id: 3},
    )
    a.extend_with_attributi_map(b)
    assert a.select_string(first_id) == "foo"
    assert a.select_int(second_id) == 3


def test_create_sub_map_for_group() -> None:
    first_id = AttributoId(1)
    second_id = AttributoId(2)
    a = AttributiMap.from_types_and_raw(
        [
            DBAttributo(
                first_id,
                BeamtimeId(1),
                "a",
                "description",
                ATTRIBUTO_GROUP_MANUAL,
                AssociatedTable.RUN,
                AttributoTypeInt(),
            ),
            DBAttributo(
                second_id,
                BeamtimeId(1),
                "b",
                "description",
                "automatic",
                AssociatedTable.RUN,
                AttributoTypeString(),
            ),
        ],
        {first_id: 3, second_id: "foo"},
    ).create_sub_map_for_group(ATTRIBUTO_GROUP_MANUAL)

    assert a.select_int(first_id) == 3
    assert a.select(second_id) is None


def test_run_matches_dataset_bool_and_string() -> None:
    first_id = AttributoId(1)
    second_id = AttributoId(2)
    attributi = (
        DBAttributo(
            first_id,
            BeamtimeId(1),
            "first",
            "description",
            ATTRIBUTO_GROUP_MANUAL,
            AssociatedTable.RUN,
            AttributoTypeBoolean(),
        ),
        DBAttributo(
            second_id,
            BeamtimeId(1),
            "second",
            "description",
            "automatic",
            AssociatedTable.RUN,
            AttributoTypeString(),
        ),
    )
    run_attributi_no_boolean = AttributiMap.from_types_and_raw(
        attributi,
        # Note: boolean is missing here!
        {second_id: "foo"},
    )
    run_attributi_boolean_false = AttributiMap.from_types_and_raw(
        attributi,
        # Note: boolean is false here!
        {first_id: False, second_id: "foo"},
    )
    run_attributi_boolean_true = AttributiMap.from_types_and_raw(
        attributi,
        {first_id: True, second_id: "foo"},
    )
    data_set_attributi_boolean_false = AttributiMap.from_types_and_raw(
        attributi,
        # Boolean has to be false (or missing!)
        {first_id: False, second_id: "foo"},
    )
    data_set_attributi_boolean_true = AttributiMap.from_types_and_raw(
        attributi,
        {first_id: True, second_id: "foo"},
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


@pytest.mark.parametrize(
    "run_value, data_set_value, tolerance, tolerance_is_absolute, matches",
    [
        (
            201.0,
            200.0,
            0.1,
            False,
            True,
        ),
        (
            201.0,
            200.0,
            0.1,
            False,
            True,
        ),
        (
            220.0,
            200.0,
            0.1,
            False,
            True,
        ),
        (
            225.0,
            200.0,
            0.1,
            False,
            False,
        ),
        (
            210.0,
            200.0,
            13,
            True,
            True,
        ),
        (
            214.0,
            200.0,
            13,
            True,
            False,
        ),
        (
            200.0,
            200.0,
            None,
            True,
            True,
        ),
        (
            200.0,
            201.0,
            None,
            True,
            False,
        ),
    ],
)
def test_decimal_attributi_match(
    run_value: float | None,
    data_set_value: float | None,
    tolerance: float | None,
    tolerance_is_absolute: bool,
    matches: bool,
) -> None:
    assert (
        decimal_attributi_match(
            AttributoTypeDecimal(
                range=None,
                suffix=None,
                standard_unit=False,
                tolerance=tolerance,
                tolerance_is_absolute=tolerance_is_absolute,
            ),
            run_value,
            data_set_value,
        )
        == matches
    )
