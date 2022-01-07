import datetime

import pytest
from isodate import duration_isoformat

from amarcord.db.attributi import convert_attributo_value
from amarcord.db.attributo_type import AttributoTypeChoice
from amarcord.db.attributo_type import AttributoTypeDateTime
from amarcord.db.attributo_type import AttributoTypeDouble
from amarcord.db.attributo_type import AttributoTypeDuration
from amarcord.db.attributo_type import AttributoTypeInt
from amarcord.db.attributo_type import AttributoTypeList
from amarcord.db.attributo_type import AttributoTypeSample
from amarcord.db.attributo_type import AttributoTypeString
from amarcord.numeric_range import NumericRange


def test_attributo_int_to_int_conversion() -> None:
    # first, no restrictions on the int, should just work
    assert convert_attributo_value(AttributoTypeInt(), AttributoTypeInt(), 1) == 1
    assert convert_attributo_value(AttributoTypeInt(), AttributoTypeInt(), -1) == -1

    # next, we convert to a non-negative integer, this might fail
    assert (
        convert_attributo_value(
            AttributoTypeInt(), AttributoTypeInt(nonNegative=True), 1
        )
        == 1
    )

    with pytest.raises(Exception):
        convert_attributo_value(
            AttributoTypeInt(), AttributoTypeInt(nonNegative=True), -1
        )

    # converting _from_ a non-negative integer is fine though
    assert (
        convert_attributo_value(
            AttributoTypeInt(nonNegative=True), AttributoTypeInt(), 1
        )
        == 1
    )


def test_attributo_int_to_string() -> None:
    # next, int to string. no problem
    assert convert_attributo_value(AttributoTypeInt(), AttributoTypeString(), 1) == "1"
    assert (
        convert_attributo_value(AttributoTypeInt(), AttributoTypeString(), -1) == "-1"
    )
    # try a bigger value so we check for thousand separators
    assert (
        convert_attributo_value(AttributoTypeInt(), AttributoTypeString(), 1000000)
        == "1000000"
    )


def test_attributo_int_to_double() -> None:
    # next int to double: no problem if the double range is proper; first, no restrictions
    assert convert_attributo_value(AttributoTypeInt(), AttributoTypeDouble(), 1) == 1.0

    # now we restrict, successfully...
    assert (
        convert_attributo_value(
            AttributoTypeInt(),
            AttributoTypeDouble(range=NumericRange(0, False, 10, False)),
            5,
        )
        == 5.0
    )
    # now we restrict, unsuccessfully...
    with pytest.raises(Exception):
        convert_attributo_value(
            AttributoTypeInt(),
            AttributoTypeDouble(range=NumericRange(0, False, 10, False)),
            20,
        )


def test_attributo_int_to_list() -> None:
    # first, without length restrictions
    assert (
        convert_attributo_value(
            AttributoTypeInt(),
            AttributoTypeList(
                sub_type=AttributoTypeInt(), min_length=None, max_length=None
            ),
            1,
        )
        == [1]
    )

    # then to a list of at least x elements
    with pytest.raises(Exception):
        convert_attributo_value(
            AttributoTypeInt(),
            AttributoTypeList(
                sub_type=AttributoTypeInt(), min_length=2, max_length=None
            ),
            1,
        )

    # then to a list of a wrong element type
    with pytest.raises(Exception):
        convert_attributo_value(
            AttributoTypeInt(),
            AttributoTypeList(
                sub_type=AttributoTypeDouble(), min_length=None, max_length=None
            ),
            1,
        )


def test_attributo_duration_to_duration() -> None:
    v = datetime.timedelta(minutes=1)
    assert (
        convert_attributo_value(
            AttributoTypeDuration(),
            AttributoTypeDuration(),
            v,
        )
        == v
    )


def test_attributo_duration_to_string() -> None:
    v = datetime.timedelta(minutes=1)
    assert (
        convert_attributo_value(
            AttributoTypeDuration(),
            AttributoTypeString(),
            v,
        )
        == str(v)
    )


def test_attributo_list_to_list() -> None:
    # first, test without length restrictions and the same contained types
    assert (
        convert_attributo_value(
            AttributoTypeList(
                sub_type=AttributoTypeDouble(), min_length=None, max_length=None
            ),
            AttributoTypeList(
                sub_type=AttributoTypeDouble(), min_length=None, max_length=None
            ),
            [1, 2, 3],
        )
        == [1, 2, 3]
    )

    # now test with loser length constraints
    assert (
        convert_attributo_value(
            AttributoTypeList(
                sub_type=AttributoTypeDouble(), min_length=5, max_length=10
            ),
            AttributoTypeList(
                sub_type=AttributoTypeDouble(), min_length=None, max_length=None
            ),
            [1, 2, 3, 4, 5, 6],
        )
        == [1, 2, 3, 4, 5, 6]
    )

    # now do stricter constraints
    with pytest.raises(Exception):
        # min
        convert_attributo_value(
            AttributoTypeList(
                sub_type=AttributoTypeDouble(), min_length=2, max_length=10
            ),
            AttributoTypeList(
                sub_type=AttributoTypeDouble(), min_length=3, max_length=10
            ),
            [1, 2],
        )

    with pytest.raises(Exception):
        # max
        convert_attributo_value(
            AttributoTypeList(
                sub_type=AttributoTypeDouble(), min_length=2, max_length=5
            ),
            AttributoTypeList(
                sub_type=AttributoTypeDouble(), min_length=2, max_length=4
            ),
            [1, 2, 3, 4, 5],
        )

    # finally, test with a value, but converting value type
    assert (
        convert_attributo_value(
            AttributoTypeList(
                sub_type=AttributoTypeDouble(), min_length=None, max_length=None
            ),
            AttributoTypeList(
                sub_type=AttributoTypeInt(), min_length=None, max_length=None
            ),
            [1, 2, 3, 4, 5],
        )
        == [1.0, 2.0, 3.0, 4.0, 5.0]
    )

    # and now with an invalid type
    with pytest.raises(Exception):
        convert_attributo_value(
            AttributoTypeList(
                sub_type=AttributoTypeDouble(), min_length=2, max_length=5
            ),
            AttributoTypeList(
                sub_type=AttributoTypeDuration(), min_length=2, max_length=4
            ),
            [1, 2, 3, 4, 5],
        )


def test_attributo_string_to_string() -> None:
    assert (
        convert_attributo_value(AttributoTypeString(), AttributoTypeString(), "foo")
        == "foo"
    )


def test_attributo_string_to_int() -> None:
    # no restrictions, valid string
    assert convert_attributo_value(AttributoTypeString(), AttributoTypeInt(), "3") == 3

    # no restrictions, invalid string
    with pytest.raises(Exception):
        convert_attributo_value(AttributoTypeString(), AttributoTypeInt(), "a")

    # restrictions, still valid
    assert (
        convert_attributo_value(
            AttributoTypeString(), AttributoTypeInt(nonNegative=True), "3"
        )
        == 3
    )

    # restrictions, invalid
    with pytest.raises(Exception):
        convert_attributo_value(
            AttributoTypeString(), AttributoTypeInt(nonNegative=True), "-3"
        )


def test_attributo_string_duration() -> None:
    t = datetime.timedelta(minutes=3)
    assert (
        convert_attributo_value(
            AttributoTypeString(), AttributoTypeDuration(), duration_isoformat(t)
        )
        == t
    )


def test_attributo_string_datetime() -> None:
    t = datetime.datetime.now()
    assert (
        convert_attributo_value(
            AttributoTypeString(), AttributoTypeDateTime(), t.isoformat()
        )
        == t
    )


def test_attributo_string_choice() -> None:
    assert (
        convert_attributo_value(
            AttributoTypeString(),
            AttributoTypeChoice(values=[("pretty", "v1"), ("pretty2", "v2")]),
            "v1",
        )
        == "v1"
    )

    with pytest.raises(Exception):
        convert_attributo_value(
            AttributoTypeString(),
            AttributoTypeChoice(values=[("pretty", "v1"), ("pretty2", "v2")]),
            "v3",
        )


def test_attributo_string_double() -> None:
    # no restrictions, valid string
    assert (
        convert_attributo_value(AttributoTypeString(), AttributoTypeDouble(), "3.5")
        == 3.5
    )

    # no restrictions, invalid string
    with pytest.raises(Exception):
        convert_attributo_value(AttributoTypeString(), AttributoTypeDouble(), "a")

    # restrictions, valid string, valid value
    assert (
        convert_attributo_value(
            AttributoTypeString(),
            AttributoTypeDouble(range=NumericRange(1.0, False, 5.0, False)),
            "3.5",
        )
        == 3.5
    )

    # restrictions, valid string, invalid value
    with pytest.raises(Exception):
        convert_attributo_value(
            AttributoTypeString(),
            AttributoTypeDouble(range=NumericRange(1.0, False, 5.0, False)),
            "13.5",
        )


def test_attributo_string_list() -> None:
    # first, without length restrictions
    assert (
        convert_attributo_value(
            AttributoTypeString(),
            AttributoTypeList(
                sub_type=AttributoTypeString(), min_length=None, max_length=None
            ),
            "abc",
        )
        == ["abc"]
    )

    # then to a list of at least x elements
    with pytest.raises(Exception):
        convert_attributo_value(
            AttributoTypeString(),
            AttributoTypeList(
                sub_type=AttributoTypeString(), min_length=2, max_length=None
            ),
            "abc",
        )

    # then to a list of a wrong element type
    with pytest.raises(Exception):
        convert_attributo_value(
            AttributoTypeString(),
            AttributoTypeList(
                sub_type=AttributoTypeDuration(), min_length=None, max_length=None
            ),
            "abc",
        )


def test_attributo_double_to_double() -> None:
    # first, without any restrictions
    assert (
        convert_attributo_value(
            AttributoTypeDouble(),
            AttributoTypeDouble(),
            1.5,
        )
        == 1.5
    )

    # now loosening restrictions
    assert (
        convert_attributo_value(
            AttributoTypeDouble(range=NumericRange(1.0, False, 5.0, False)),
            AttributoTypeDouble(range=NumericRange(0.0, False, 10.0, False)),
            1.5,
        )
        == 1.5
    )

    # now tightening, but still valid restrictions
    assert (
        convert_attributo_value(
            AttributoTypeDouble(range=NumericRange(1.0, False, 5.0, False)),
            AttributoTypeDouble(range=NumericRange(3.0, False, 5.0, False)),
            3.5,
        )
        == 3.5
    )

    # now tightening, but invalid restrictions
    with pytest.raises(Exception):
        assert (
            convert_attributo_value(
                AttributoTypeDouble(range=NumericRange(1.0, False, 5.0, False)),
                AttributoTypeDouble(range=NumericRange(3.0, False, 5.0, False)),
                1.5,
            )
            == 1.5
        )

    # now unit conversion
    assert (
        convert_attributo_value(
            AttributoTypeDouble(standard_unit=True, suffix="MHz"),
            AttributoTypeDouble(standard_unit=True, suffix="kHz"),
            1.5,
        )
        == 1500.0
    )

    # now invalid unit conversion
    with pytest.raises(Exception):
        convert_attributo_value(
            AttributoTypeDouble(standard_unit=True, suffix="MHz"),
            AttributoTypeDouble(standard_unit=True, suffix="degF"),
            1.5,
        )


def test_attributo_double_to_int() -> None:
    # first, without any restrictions
    assert (
        convert_attributo_value(
            AttributoTypeDouble(),
            AttributoTypeInt(),
            1.5,
        )
        == 1
    )

    # now with restrictions, valid
    assert (
        convert_attributo_value(
            AttributoTypeDouble(),
            AttributoTypeInt(nonNegative=True),
            1.5,
        )
        == 1
    )

    # now with restrictions, invalid
    with pytest.raises(Exception):
        convert_attributo_value(
            AttributoTypeDouble(),
            AttributoTypeInt(nonNegative=True),
            -1.5,
        )

    # now with restrictions on the range, valid
    assert (
        convert_attributo_value(
            AttributoTypeDouble(),
            AttributoTypeInt(range=(10, 20)),
            15.5,
        )
        == 15
    )

    # now with restrictions, invalid
    with pytest.raises(Exception):
        convert_attributo_value(
            AttributoTypeDouble(),
            AttributoTypeInt(range=(10, 20)),
            30.0,
        )


def test_attributo_double_list() -> None:
    # first, without length restrictions
    assert (
        convert_attributo_value(
            AttributoTypeDouble(),
            AttributoTypeList(
                sub_type=AttributoTypeDouble(), min_length=None, max_length=None
            ),
            1.5,
        )
        == [1.5]
    )

    # then to a list of at least x elements
    with pytest.raises(Exception):
        convert_attributo_value(
            AttributoTypeDouble(),
            AttributoTypeList(
                sub_type=AttributoTypeDouble(), min_length=2, max_length=None
            ),
            1.0,
        )

    # then to a list of a wrong element type
    with pytest.raises(Exception):
        convert_attributo_value(
            AttributoTypeDouble(),
            AttributoTypeList(
                sub_type=AttributoTypeDuration(), min_length=None, max_length=None
            ),
            1,
        )


def test_attributo_double_string() -> None:
    assert (
        convert_attributo_value(
            AttributoTypeDouble(),
            AttributoTypeString(),
            1.5,
        )
        == "1.5"
    )

    # Big value to check thousands separators
    assert (
        convert_attributo_value(
            AttributoTypeDouble(),
            AttributoTypeString(),
            1000000.0,
        )
        == "1000000.0"
    )


def test_attributo_sample_to_sample() -> None:
    assert (
        convert_attributo_value(
            AttributoTypeSample(),
            AttributoTypeSample(),
            1,
        )
        == 1
    )


def test_attributo_datetime_to_datetime() -> None:
    d = datetime.datetime.utcnow()
    assert (
        convert_attributo_value(
            AttributoTypeDateTime(),
            AttributoTypeDateTime(),
            d,
        )
        == d
    )


def test_attributo_datetime_to_string() -> None:
    d = datetime.datetime.utcnow()
    assert (
        convert_attributo_value(
            AttributoTypeDateTime(),
            AttributoTypeString(),
            d,
        )
        == d.isoformat()
    )


def test_attributo_choice_to_choice() -> None:
    # first, valid choice to same choice
    assert (
        convert_attributo_value(
            AttributoTypeChoice(values=[("pretty1", "v1"), ("pretty2", "v2")]),
            AttributoTypeChoice(values=[("pretty1", "v1"), ("pretty2", "v2")]),
            "v1",
        )
        == "v1"
    )

    # then loosen choices
    assert (
        convert_attributo_value(
            AttributoTypeChoice(values=[("pretty1", "v1"), ("pretty2", "v2")]),
            AttributoTypeChoice(
                values=[("pretty1", "v1"), ("pretty2", "v2"), ("pretty3", "v3")]
            ),
            "v1",
        )
        == "v1"
    )

    # now tighten choices but stay valid
    assert (
        convert_attributo_value(
            AttributoTypeChoice(values=[("pretty1", "v1"), ("pretty2", "v2")]),
            AttributoTypeChoice(values=[("pretty1", "v1")]),
            "v1",
        )
        == "v1"
    )

    # now tighten choices but become invalid
    with pytest.raises(Exception):
        convert_attributo_value(
            AttributoTypeChoice(values=[("pretty1", "v1"), ("pretty2", "v2")]),
            AttributoTypeChoice(values=[("pretty1", "v1")]),
            "v2",
        )


def test_attributo_choice_to_string() -> None:
    assert (
        convert_attributo_value(
            AttributoTypeChoice(values=[("pretty1", "v1"), ("pretty2", "v2")]),
            AttributoTypeString(),
            "v1",
        )
        == "v1"
    )
