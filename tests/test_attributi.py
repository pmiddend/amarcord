import pytest

from amarcord.db.attributi import attributo_type_to_schema
from amarcord.db.attributi import attributo_type_to_string
from amarcord.db.attributi import attributo_types_semantically_equivalent
from amarcord.db.attributo_type import AttributoType
from amarcord.db.attributo_type import AttributoTypeBoolean
from amarcord.db.attributo_type import AttributoTypeChemical
from amarcord.db.attributo_type import AttributoTypeChoice
from amarcord.db.attributo_type import AttributoTypeDateTime
from amarcord.db.attributo_type import AttributoTypeDecimal
from amarcord.db.attributo_type import AttributoTypeInt
from amarcord.db.attributo_type import AttributoTypeList
from amarcord.db.attributo_type import AttributoTypeString
from amarcord.json_schema import coparse_schema_type
from amarcord.json_types import JSONDict
from amarcord.numeric_range import NumericRange


def test_attributo_type_to_string_int() -> None:
    assert attributo_type_to_string(AttributoTypeInt()) == "integer"


def test_attributo_type_to_string_boolean() -> None:
    assert attributo_type_to_string(AttributoTypeBoolean()) == "yes/no"


def test_attributo_type_to_string_string() -> None:
    assert attributo_type_to_string(AttributoTypeString()) == "string"


def test_attributo_type_to_string_chemical() -> None:
    assert attributo_type_to_string(AttributoTypeChemical()) == "chemical ID"


def test_attributo_type_to_string_choice() -> None:
    assert (
        attributo_type_to_string(AttributoTypeChoice(values=["a", "b"]))
        == "one of a, b"
    )


def test_attributo_type_to_string_decimal() -> None:
    assert attributo_type_to_string(AttributoTypeDecimal()) == "number"
    # Only a range
    assert (
        attributo_type_to_string(
            AttributoTypeDecimal(
                range=NumericRange(
                    minimum=3,
                    minimum_inclusive=False,
                    maximum=4,
                    maximum_inclusive=True,
                )
            )
        )
        == "number ∈ (3,4]"
    )
    # Only a suffix
    assert attributo_type_to_string(AttributoTypeDecimal(suffix="mm")) == "mm"
    # Range and suffix
    assert (
        attributo_type_to_string(
            AttributoTypeDecimal(
                range=NumericRange(
                    minimum=3,
                    minimum_inclusive=False,
                    maximum=4,
                    maximum_inclusive=True,
                ),
                suffix="mm",
            )
        )
        == "mm ∈ (3,4]"
    )


def test_attributo_type_to_string_date_time() -> None:
    assert attributo_type_to_string(AttributoTypeDateTime()) == "date-time"


def test_attributo_type_to_string_list() -> None:
    assert (
        attributo_type_to_string(
            AttributoTypeList(
                sub_type=AttributoTypeString(), min_length=None, max_length=None
            )
        )
        == "list of string"
    )


def test_attributo_types_semantically_equivalent() -> None:
    assert attributo_types_semantically_equivalent(
        AttributoTypeInt(), AttributoTypeInt()
    )
    assert not attributo_types_semantically_equivalent(
        AttributoTypeInt(), AttributoTypeString()
    )
    assert attributo_types_semantically_equivalent(
        AttributoTypeDecimal(range=None, suffix=None, standard_unit=False),
        AttributoTypeDecimal(range=None, suffix=None, standard_unit=False),
    )
    # Range differs
    assert not attributo_types_semantically_equivalent(
        AttributoTypeDecimal(
            range=NumericRange(
                minimum=1, minimum_inclusive=False, maximum=2, maximum_inclusive=False
            ),
            suffix=None,
            standard_unit=False,
        ),
        AttributoTypeDecimal(
            range=NumericRange(
                # Maximum 3 here, 2 above
                minimum=1,
                minimum_inclusive=False,
                maximum=3,
                maximum_inclusive=False,
            ),
            suffix=None,
            standard_unit=False,
        ),
    )
    # Range does not differ
    assert attributo_types_semantically_equivalent(
        AttributoTypeDecimal(
            range=NumericRange(
                minimum=1, minimum_inclusive=False, maximum=2, maximum_inclusive=False
            ),
            suffix=None,
            standard_unit=False,
        ),
        AttributoTypeDecimal(
            range=NumericRange(
                minimum=1, minimum_inclusive=False, maximum=2, maximum_inclusive=False
            ),
            suffix=None,
            standard_unit=False,
        ),
    )
    # Suffix same, no standard unit
    assert attributo_types_semantically_equivalent(
        AttributoTypeDecimal(
            range=None,
            suffix="abc",
            standard_unit=False,
        ),
        AttributoTypeDecimal(
            range=None,
            suffix="abc",
            standard_unit=False,
        ),
    )
    # Suffix differs, no standard unit
    assert not attributo_types_semantically_equivalent(
        AttributoTypeDecimal(
            range=None,
            suffix="abc",
            standard_unit=False,
        ),
        AttributoTypeDecimal(
            range=None,
            suffix="abcd",
            standard_unit=False,
        ),
    )
    # Suffix differs, same standard unit, written with spaces, so equivalent
    assert attributo_types_semantically_equivalent(
        AttributoTypeDecimal(
            range=None,
            suffix="mm/s",
            standard_unit=True,
        ),
        AttributoTypeDecimal(
            range=None,
            suffix="mm / s",
            standard_unit=True,
        ),
    )
    # Suffix exactly same
    assert attributo_types_semantically_equivalent(
        AttributoTypeDecimal(
            range=None,
            suffix="mm/s",
            standard_unit=True,
        ),
        AttributoTypeDecimal(
            range=None,
            suffix="mm/s",
            standard_unit=True,
        ),
    )
    # Suffix really different unit
    assert not attributo_types_semantically_equivalent(
        AttributoTypeDecimal(
            range=None,
            suffix="mm/s",
            standard_unit=True,
        ),
        AttributoTypeDecimal(
            range=None,
            suffix="mm/ms",
            standard_unit=True,
        ),
    )


@pytest.mark.parametrize(
    "test_input,expected",
    [
        (
            AttributoTypeDecimal(
                range=None,
                suffix=None,
                standard_unit=False,
                tolerance_is_absolute=False,
                tolerance=0.5,
            ),
            {"type": "number", "tolerance": 0.5, "toleranceIsAbsolute": False},
        ),
        (
            AttributoTypeDecimal(
                range=None,
                suffix=None,
                standard_unit=False,
                tolerance_is_absolute=True,
                tolerance=0.5,
            ),
            {"type": "number", "tolerance": 0.5, "toleranceIsAbsolute": True},
        ),
        (
            AttributoTypeDecimal(
                range=None,
                suffix=None,
                standard_unit=False,
                tolerance_is_absolute=True,
                tolerance=None,
            ),
            {"type": "number"},
        ),
        (
            AttributoTypeDecimal(
                range=NumericRange(
                    minimum=1.0,
                    maximum=3.0,
                    minimum_inclusive=False,
                    maximum_inclusive=False,
                ),
                suffix=None,
                standard_unit=False,
                tolerance_is_absolute=True,
                tolerance=None,
            ),
            {"type": "number", "exclusiveMinimum": 1.0, "exclusiveMaximum": 3.0},
        ),
        (
            AttributoTypeDecimal(
                range=NumericRange(
                    minimum=1.0,
                    maximum=3.0,
                    minimum_inclusive=True,
                    maximum_inclusive=False,
                ),
                suffix=None,
                standard_unit=False,
                tolerance_is_absolute=True,
                tolerance=None,
            ),
            {"type": "number", "minimum": 1.0, "exclusiveMaximum": 3.0},
        ),
        (
            AttributoTypeDecimal(
                range=NumericRange(
                    minimum=1.0,
                    maximum=3.0,
                    minimum_inclusive=False,
                    maximum_inclusive=True,
                ),
                suffix=None,
                standard_unit=False,
                tolerance_is_absolute=True,
                tolerance=None,
            ),
            {"type": "number", "exclusiveMinimum": 1.0, "maximum": 3.0},
        ),
        (
            AttributoTypeDecimal(
                range=NumericRange(
                    minimum=1.0,
                    maximum=3.0,
                    minimum_inclusive=True,
                    maximum_inclusive=True,
                ),
                suffix=None,
                standard_unit=False,
                tolerance_is_absolute=True,
                tolerance=None,
            ),
            {"type": "number", "minimum": 1.0, "maximum": 3.0},
        ),
    ],
)
def test_attributo_type_to_schema(
    test_input: AttributoType, expected: JSONDict
) -> None:
    assert coparse_schema_type(attributo_type_to_schema(test_input)) == expected
