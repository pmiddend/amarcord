import datetime
from dataclasses import replace
from typing import Final

import pytest

from amarcord.db.attributi import AttributoConversionFlags
from amarcord.db.attributi import convert_attributo_value
from amarcord.db.attributi import datetime_to_attributo_int
from amarcord.db.attributi import datetime_to_attributo_string
from amarcord.db.attributo_type import AttributoTypeBoolean
from amarcord.db.attributo_type import AttributoTypeChemical
from amarcord.db.attributo_type import AttributoTypeChoice
from amarcord.db.attributo_type import AttributoTypeDateTime
from amarcord.db.attributo_type import AttributoTypeDecimal
from amarcord.db.attributo_type import AttributoTypeInt
from amarcord.db.attributo_type import AttributoTypeList
from amarcord.db.attributo_type import AttributoTypeString
from amarcord.numeric_range import NumericRange

_default_flags: Final = AttributoConversionFlags(ignore_units=False)


def test_attributo_int_to_int_conversion() -> None:
    # first, no restrictions on the int, should just work
    assert (
        convert_attributo_value(
            AttributoTypeInt(), AttributoTypeInt(), _default_flags, 1
        )
        == 1
    )
    assert (
        convert_attributo_value(
            AttributoTypeInt(), AttributoTypeInt(), _default_flags, -1
        )
        == -1
    )


def test_attributo_int_to_string() -> None:
    # next, int to string. no problem
    assert (
        convert_attributo_value(
            AttributoTypeInt(), AttributoTypeString(), _default_flags, 1
        )
        == "1"
    )
    assert (
        convert_attributo_value(
            AttributoTypeInt(), AttributoTypeString(), _default_flags, -1
        )
        == "-1"
    )
    # try a bigger value so we check for thousand separators
    assert (
        convert_attributo_value(
            AttributoTypeInt(), AttributoTypeString(), _default_flags, 1000000
        )
        == "1000000"
    )


def test_attributo_int_to_double() -> None:
    # next int to double: no problem if the double range is proper; first, no restrictions
    assert (
        convert_attributo_value(
            AttributoTypeInt(), AttributoTypeDecimal(), _default_flags, 1
        )
        == 1.0
    )

    # now we restrict, successfully...
    assert (
        convert_attributo_value(
            AttributoTypeInt(),
            AttributoTypeDecimal(range=NumericRange(0, False, 10, False)),
            _default_flags,
            5,
        )
        == 5.0
    )
    # now we restrict, unsuccessfully...
    with pytest.raises(Exception):
        convert_attributo_value(
            AttributoTypeInt(),
            AttributoTypeDecimal(range=NumericRange(0, False, 10, False)),
            _default_flags,
            20,
        )


def test_attributo_int_to_list() -> None:
    # first, without length restrictions
    assert convert_attributo_value(
        AttributoTypeInt(),
        AttributoTypeList(
            sub_type=AttributoTypeInt(), min_length=None, max_length=None
        ),
        _default_flags,
        1,
    ) == [1]

    # then to a list of at least x elements
    with pytest.raises(Exception):
        convert_attributo_value(
            AttributoTypeInt(),
            AttributoTypeList(
                sub_type=AttributoTypeInt(), min_length=2, max_length=None
            ),
            _default_flags,
            1,
        )

    # then to a list of a wrong element type
    with pytest.raises(Exception):
        convert_attributo_value(
            AttributoTypeInt(),
            AttributoTypeList(
                sub_type=AttributoTypeDecimal(), min_length=None, max_length=None
            ),
            _default_flags,
            1,
        )


def test_attributo_list_to_list() -> None:
    # first, test without length restrictions and the same contained types
    assert convert_attributo_value(
        AttributoTypeList(
            sub_type=AttributoTypeDecimal(), min_length=None, max_length=None
        ),
        AttributoTypeList(
            sub_type=AttributoTypeDecimal(), min_length=None, max_length=None
        ),
        _default_flags,
        [1, 2, 3],
    ) == [1, 2, 3]

    # now test with loser length constraints
    assert convert_attributo_value(
        AttributoTypeList(sub_type=AttributoTypeDecimal(), min_length=5, max_length=10),
        AttributoTypeList(
            sub_type=AttributoTypeDecimal(), min_length=None, max_length=None
        ),
        _default_flags,
        [1, 2, 3, 4, 5, 6],
    ) == [1, 2, 3, 4, 5, 6]

    # now do stricter constraints
    with pytest.raises(Exception):
        # min
        convert_attributo_value(
            AttributoTypeList(
                sub_type=AttributoTypeDecimal(), min_length=2, max_length=10
            ),
            AttributoTypeList(
                sub_type=AttributoTypeDecimal(), min_length=3, max_length=10
            ),
            _default_flags,
            [1, 2],
        )

    with pytest.raises(Exception):
        # max
        convert_attributo_value(
            AttributoTypeList(
                sub_type=AttributoTypeDecimal(), min_length=2, max_length=5
            ),
            AttributoTypeList(
                sub_type=AttributoTypeDecimal(), min_length=2, max_length=4
            ),
            _default_flags,
            [1, 2, 3, 4, 5],
        )

    # finally, test with a value, but converting value type
    assert convert_attributo_value(
        AttributoTypeList(
            sub_type=AttributoTypeDecimal(), min_length=None, max_length=None
        ),
        AttributoTypeList(
            sub_type=AttributoTypeInt(), min_length=None, max_length=None
        ),
        _default_flags,
        [1, 2, 3, 4, 5],
    ) == [1.0, 2.0, 3.0, 4.0, 5.0]

    # and now with an invalid type
    with pytest.raises(Exception):
        convert_attributo_value(
            AttributoTypeList(
                sub_type=AttributoTypeDecimal(), min_length=2, max_length=5
            ),
            AttributoTypeList(
                sub_type=AttributoTypeDateTime(), min_length=2, max_length=4
            ),
            _default_flags,
            [1, 2, 3, 4, 5],
        )


def test_attributo_string_to_string() -> None:
    assert (
        convert_attributo_value(
            AttributoTypeString(), AttributoTypeString(), _default_flags, "foo"
        )
        == "foo"
    )


def test_attributo_string_to_int() -> None:
    # no restrictions, valid string
    assert (
        convert_attributo_value(
            AttributoTypeString(), AttributoTypeInt(), _default_flags, "3"
        )
        == 3
    )

    # no restrictions, invalid string
    with pytest.raises(Exception):
        convert_attributo_value(
            AttributoTypeString(), AttributoTypeInt(), _default_flags, "a"
        )


def test_attributo_string_datetime() -> None:
    # We are not storing microseconds in the JSON (not needed until now)
    t = datetime.datetime.now().replace(microsecond=0)
    assert convert_attributo_value(
        AttributoTypeString(),
        AttributoTypeDateTime(),
        _default_flags,
        datetime_to_attributo_string(t),
    ) == datetime_to_attributo_int(t)


def test_attributo_string_choice() -> None:
    assert (
        convert_attributo_value(
            AttributoTypeString(),
            AttributoTypeChoice(values=["v1", "v2"]),
            _default_flags,
            "v1",
        )
        == "v1"
    )

    with pytest.raises(Exception):
        convert_attributo_value(
            AttributoTypeString(),
            AttributoTypeChoice(values=["v1", "v2"]),
            _default_flags,
            "v3",
        )


def test_attributo_string_double() -> None:
    # no restrictions, valid string
    assert (
        convert_attributo_value(
            AttributoTypeString(), AttributoTypeDecimal(), _default_flags, "3.5"
        )
        == 3.5
    )

    # no restrictions, invalid string
    with pytest.raises(Exception):
        convert_attributo_value(
            AttributoTypeString(), AttributoTypeDecimal(), _default_flags, "a"
        )

    # restrictions, valid string, valid value
    assert (
        convert_attributo_value(
            AttributoTypeString(),
            AttributoTypeDecimal(range=NumericRange(1.0, False, 5.0, False)),
            _default_flags,
            "3.5",
        )
        == 3.5
    )

    # restrictions, valid string, invalid value
    with pytest.raises(Exception):
        convert_attributo_value(
            AttributoTypeString(),
            AttributoTypeDecimal(range=NumericRange(1.0, False, 5.0, False)),
            _default_flags,
            "13.5",
        )


def test_attributo_string_list() -> None:
    # first, without length restrictions
    assert convert_attributo_value(
        AttributoTypeString(),
        AttributoTypeList(
            sub_type=AttributoTypeString(), min_length=None, max_length=None
        ),
        _default_flags,
        "abc",
    ) == ["abc"]

    # then to a list of at least x elements
    with pytest.raises(Exception):
        convert_attributo_value(
            AttributoTypeString(),
            AttributoTypeList(
                sub_type=AttributoTypeString(), min_length=2, max_length=None
            ),
            _default_flags,
            "abc",
        )

    # then to a list of a wrong element type
    with pytest.raises(Exception):
        convert_attributo_value(
            AttributoTypeString(),
            AttributoTypeList(
                sub_type=AttributoTypeDateTime(), min_length=None, max_length=None
            ),
            _default_flags,
            "abc",
        )


def test_attributo_double_to_double() -> None:
    # first, without any restrictions
    assert (
        convert_attributo_value(
            AttributoTypeDecimal(),
            AttributoTypeDecimal(),
            _default_flags,
            1.5,
        )
        == 1.5
    )

    # now loosening restrictions
    assert (
        convert_attributo_value(
            AttributoTypeDecimal(range=NumericRange(1.0, False, 5.0, False)),
            AttributoTypeDecimal(range=NumericRange(0.0, False, 10.0, False)),
            _default_flags,
            1.5,
        )
        == 1.5
    )

    # now tightening, but still valid restrictions
    assert (
        convert_attributo_value(
            AttributoTypeDecimal(range=NumericRange(1.0, False, 5.0, False)),
            AttributoTypeDecimal(range=NumericRange(3.0, False, 5.0, False)),
            _default_flags,
            3.5,
        )
        == 3.5
    )

    # now tightening, but invalid restrictions
    with pytest.raises(Exception):
        assert (
            convert_attributo_value(
                AttributoTypeDecimal(range=NumericRange(1.0, False, 5.0, False)),
                AttributoTypeDecimal(range=NumericRange(3.0, False, 5.0, False)),
                _default_flags,
                1.5,
            )
            == 1.5
        )

    # now unit conversion
    assert (
        convert_attributo_value(
            AttributoTypeDecimal(standard_unit=True, suffix="MHz"),
            AttributoTypeDecimal(standard_unit=True, suffix="kHz"),
            _default_flags,
            1.5,
        )
        == 1500.0
    )

    # double conversion with units, but without actual conversion
    assert (
        convert_attributo_value(
            AttributoTypeDecimal(standard_unit=True, suffix="MHz"),
            AttributoTypeDecimal(standard_unit=True, suffix="kHz"),
            replace(_default_flags, ignore_units=True),
            1.5,
        )
        == 1.5
    )

    # now invalid unit conversion
    with pytest.raises(Exception):
        convert_attributo_value(
            AttributoTypeDecimal(standard_unit=True, suffix="MHz"),
            AttributoTypeDecimal(standard_unit=True, suffix="degF"),
            _default_flags,
            1.5,
        )

    with pytest.raises(Exception):
        # unit conversion with a range requirement
        assert (
            convert_attributo_value(
                AttributoTypeDecimal(standard_unit=True, suffix="MHz"),
                AttributoTypeDecimal(
                    standard_unit=True,
                    suffix="kHz",
                    range=NumericRange(3.0, False, 5.0, False),
                ),
                _default_flags,
                1.5,
            )
            == 1500.0
        )


def test_attributo_double_to_int() -> None:
    # first, without any restrictions
    assert (
        convert_attributo_value(
            AttributoTypeDecimal(),
            AttributoTypeInt(),
            _default_flags,
            1.5,
        )
        == 1
    )


def test_attributo_double_list() -> None:
    # first, without length restrictions
    assert convert_attributo_value(
        AttributoTypeDecimal(),
        AttributoTypeList(
            sub_type=AttributoTypeDecimal(), min_length=None, max_length=None
        ),
        _default_flags,
        1.5,
    ) == [1.5]

    # then to a list of at least x elements
    with pytest.raises(Exception):
        convert_attributo_value(
            AttributoTypeDecimal(),
            AttributoTypeList(
                sub_type=AttributoTypeDecimal(), min_length=2, max_length=None
            ),
            _default_flags,
            1.0,
        )

    # then to a list of a wrong element type
    with pytest.raises(Exception):
        convert_attributo_value(
            AttributoTypeDecimal(),
            AttributoTypeList(
                sub_type=AttributoTypeDateTime(), min_length=None, max_length=None
            ),
            _default_flags,
            1,
        )


def test_attributo_double_string() -> None:
    assert (
        convert_attributo_value(
            AttributoTypeDecimal(),
            AttributoTypeString(),
            _default_flags,
            1.5,
        )
        == "1.5"
    )

    # Big value to check thousands separators
    assert (
        convert_attributo_value(
            AttributoTypeDecimal(),
            AttributoTypeString(),
            _default_flags,
            1000000.0,
        )
        == "1000000.0"
    )


def test_attributo_chemical_to_chemical() -> None:
    assert (
        convert_attributo_value(
            AttributoTypeChemical(),
            AttributoTypeChemical(),
            _default_flags,
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
            _default_flags,
            d,
        )
        == d
    )


def test_attributo_datetime_to_string() -> None:
    d = datetime.datetime.utcnow()
    assert convert_attributo_value(
        AttributoTypeDateTime(),
        AttributoTypeString(),
        _default_flags,
        d,
    ) == datetime_to_attributo_string(d)


def test_attributo_choice_to_choice() -> None:
    # first, valid choice to same choice
    assert (
        convert_attributo_value(
            AttributoTypeChoice(values=["v1", "v2"]),
            AttributoTypeChoice(values=["v1", "v2"]),
            _default_flags,
            "v1",
        )
        == "v1"
    )

    # then loosen choices
    assert (
        convert_attributo_value(
            AttributoTypeChoice(values=["v1", "v2"]),
            AttributoTypeChoice(values=["v1", "v2", "v3"]),
            _default_flags,
            "v1",
        )
        == "v1"
    )

    # now tighten choices but stay valid
    assert (
        convert_attributo_value(
            AttributoTypeChoice(values=["v1", "v1"]),
            AttributoTypeChoice(values=["v1"]),
            _default_flags,
            "v1",
        )
        == "v1"
    )

    # now tighten choices but become invalid
    with pytest.raises(Exception):
        convert_attributo_value(
            AttributoTypeChoice(values=["v1", "v1"]),
            AttributoTypeChoice(values=["v1"]),
            _default_flags,
            "v2",
        )


def test_attributo_choice_to_string() -> None:
    assert (
        convert_attributo_value(
            AttributoTypeChoice(values=["v1", "v1"]),
            AttributoTypeString(),
            _default_flags,
            "v1",
        )
        == "v1"
    )


def test_attributo_boolean_to_int() -> None:
    assert (
        convert_attributo_value(
            AttributoTypeBoolean(),
            AttributoTypeInt(),
            _default_flags,
            True,
        )
        == 1
    )
    assert (
        convert_attributo_value(
            AttributoTypeBoolean(),
            AttributoTypeInt(),
            _default_flags,
            False,
        )
        == 0
    )


def test_attributo_boolean_to_double() -> None:
    assert (
        convert_attributo_value(
            AttributoTypeBoolean(),
            AttributoTypeDecimal(),
            _default_flags,
            True,
        )
        == 1.0
    )
    assert (
        convert_attributo_value(
            AttributoTypeBoolean(),
            AttributoTypeDecimal(),
            _default_flags,
            False,
        )
        == 0.0
    )
    with pytest.raises(Exception):
        convert_attributo_value(
            AttributoTypeBoolean(),
            AttributoTypeDecimal(
                range=NumericRange(
                    minimum=20,
                    minimum_inclusive=False,
                    maximum=None,
                    maximum_inclusive=False,
                )
            ),
            _default_flags,
            False,
        )


def test_attributo_boolean_to_string() -> None:
    assert (
        convert_attributo_value(
            AttributoTypeBoolean(),
            AttributoTypeString(),
            _default_flags,
            True,
        )
        == "True"
    )

    assert (
        convert_attributo_value(
            AttributoTypeBoolean(),
            AttributoTypeString(),
            _default_flags,
            False,
        )
        == "False"
    )


def test_attributo_string_to_boolean() -> None:
    assert (
        # pylint: disable=singleton-comparison
        convert_attributo_value(
            AttributoTypeString(),
            AttributoTypeBoolean(),
            _default_flags,
            "True",
        )
        == True
    )

    assert (
        # pylint: disable=singleton-comparison
        convert_attributo_value(
            AttributoTypeString(),
            AttributoTypeBoolean(),
            _default_flags,
            "yes",
        )
        == True
    )
