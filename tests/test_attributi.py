from amarcord.db.attributi import attributo_type_to_string
from amarcord.db.attributo_type import (
    AttributoTypeInt,
    AttributoTypeBoolean,
    AttributoTypeString,
    AttributoTypeSample,
    AttributoTypeChoice,
    AttributoTypeDecimal,
    AttributoTypeDateTime,
    AttributoTypeList,
)
from amarcord.numeric_range import NumericRange


def test_attributo_type_to_string_int() -> None:
    assert attributo_type_to_string(AttributoTypeInt()) == "integer"


def test_attributo_type_to_string_boolean() -> None:
    assert attributo_type_to_string(AttributoTypeBoolean()) == "yes/no"


def test_attributo_type_to_string_string() -> None:
    assert attributo_type_to_string(AttributoTypeString()) == "string"


def test_attributo_type_to_string_sample() -> None:
    assert attributo_type_to_string(AttributoTypeSample()) == "Sample ID"


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
